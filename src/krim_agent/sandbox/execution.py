"""已审批命令的控制面意图、轮询、取消与真实结果登记。"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import TYPE_CHECKING, Any, cast

from pydantic import JsonValue
from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.runtime.contracts import settled
from krim_agent.sandbox.command import SandboxCommand
from krim_agent.sandbox.file_models import FileChange, FilePatch, FileRemove, FileRename, FileWrite
from krim_agent.sandbox.operations import SessionCommandScope
from krim_agent.security import PrincipalContext
from krim_agent.storage.models import runs
from krim_agent.storage.sandbox_models import sandbox_operations as operations
from krim_agent.storage.sandbox_models import sandbox_projects as projects
from krim_agent.tools.core import Invocation
from krim_agent.tools.pipeline import CURRENT

if TYPE_CHECKING:
    from krim_agent.sandbox.service import SandboxService


def request_key(invocation: Invocation) -> str:
    return "command:" + fingerprint({"run": invocation.run_id, "call": invocation.call_id})


async def execute_command(
    service: SandboxService,
    actor: PrincipalContext,
    values: SandboxCommand | FileChange,
    invocation: Invocation,
    scope: SessionCommandScope,
) -> dict[str, Any]:
    if (
        cast(object, CURRENT.get()) is not scope
        or scope.invocation != invocation
        or scope.operations.service is not service
    ):
        raise ServiceError("workspace_contract_required")
    broker = service.broker
    if broker is None or service.configuration.command_image is None:
        raise ServiceError("sandbox_backend_unavailable", 503)
    scope.check()
    payload = values.model_dump(mode="json")
    digest = fingerprint(
        {
            "run": invocation.run_id,
            "call": invocation.call_id,
            "tool_hash": invocation.tool_hash,
            "command": payload,
        }
    )

    async def reserve(conn: AsyncConnection) -> dict[str, Any]:
        await service._authorize(conn, actor)
        scope.check()
        from krim_agent.storage.sandbox_models import sandbox_quota_state

        quota_state = (
            (
                await conn.execute(
                    select(sandbox_quota_state).where(
                        sandbox_quota_state.c.principal_id == actor.principal_id
                    )
                )
            )
            .mappings()
            .first()
        )
        if quota_state is not None and quota_state["state"] not in {
            "applied",
            "cancelled",
            "failed_validation",
        }:
            raise ServiceError("workspace_busy")
        from krim_agent.sandbox.bindings import binding_row

        selected = await binding_row(conn, scope.session_id)
        if (
            (selected["revision"] if selected else 0) != scope.binding_revision
            or (selected["project_id"] if selected else None) != scope.project_id
            or scope.project_id is not None
            and values.project_id != scope.project_id
        ):
            raise ServiceError("target_binding_changed")
        old = (
            (
                await conn.execute(
                    select(operations).where(
                        operations.c.principal_id == actor.principal_id,
                        operations.c.request_key == request_key(invocation),
                    )
                )
            )
            .mappings()
            .first()
        )
        if old is not None:
            if old["request_hash"] != digest:
                raise ServiceError("key_conflict")
            if old["result"]["authorization_generation"] != actor.generation:
                raise ServiceError("permission_denied")
            return dict(old)
        project = (
            (
                await conn.execute(
                    select(projects).where(
                        projects.c.project_id == values.project_id,
                        projects.c.principal_id == actor.principal_id,
                    )
                )
            )
            .mappings()
            .first()
        )
        if project is None:
            raise ServiceError("not_found", 404)
        if project["workspace_id"] != scope.workspace_id:
            raise ServiceError("permission_denied")
        actor.member(project["workspace_id"], write=True)
        if project["generation"] != values.expected_generation:
            raise ServiceError("input_revision_conflict")
        if project["status"] != "ready" or project["lease_id"] is not None:
            raise ServiceError("workspace_busy")
        if (
            await conn.scalar(select(runs.c.status).where(runs.c.id == invocation.run_id))
            != "running"
        ):
            raise ServiceError("run_closed")
        operation_id = str(uuid.uuid4())
        intent = {
            "project_id": values.project_id,
            "environment_id": project["environment_id"],
            "run_id": invocation.run_id,
            "call_id": invocation.call_id,
            "tool_hash": invocation.tool_hash,
            "command": payload,
            "authorization_generation": actor.generation,
            "image": service.configuration.command_image,
        }
        now = time.time()
        await conn.execute(
            insert(operations).values(
                operation_id=operation_id,
                principal_id=actor.principal_id,
                request_key=request_key(invocation),
                request_hash=digest,
                project_id=values.project_id,
                kind="command",
                status="intent",
                result=intent,
                created_at=now,
                updated_at=now,
            )
        )
        await conn.execute(
            update(projects)
            .where(projects.c.project_id == values.project_id)
            .values(status="running", lease_id=operation_id, lease_expires_at=now + 90)
        )
        return {"operation_id": operation_id, "status": "intent", "result": intent, "new": True}

    accepted = await settled(service.store._write(reserve))
    intent, operation_id = accepted["result"], accepted["operation_id"]
    if accepted["status"] == "completed":
        return cast(dict[str, Any], intent["receipt"])
    args = {
        "environment_id": intent["environment_id"],
        "project_id": values.project_id,
        "operation_id": operation_id,
    }
    started_recorded = False
    result: dict[str, Any] | None = None
    interrupted: BaseException | None = None
    submission_attempted = not accepted.get("new", False)
    try:
        scope.check()
        if accepted.get("new"):
            health = await broker.call("health")
            if not health.get("execution_available") or health.get("image") != intent["image"]:
                raise ServiceError("target_binding_changed")
            scope.check()
            submission_attempted = True
            if isinstance(values, SandboxCommand):
                result = await broker.call("command_start", **args, command=payload)
            else:
                kind = (
                    "patch"
                    if isinstance(values, FilePatch)
                    else "rename"
                    if isinstance(values, FileRename)
                    else "write"
                    if isinstance(values, FileWrite)
                    else "remove"
                    if isinstance(values, FileRemove)
                    else "mkdir"
                )
                result = await broker.call("file_start", **args, mutation=payload, kind=kind)
        while result is None or result["status"] not in {"finished", "unknown"}:
            scope.check()
            result = await broker.call("command_status", **args, renew=True, cancel=False)
            if result.get("executor_started") and not started_recorded:
                await service.store.append(
                    invocation.run_id,
                    "sandbox_started",
                    {
                        "call_id": invocation.call_id,
                        "project_id": values.project_id,
                        "operation_id": operation_id,
                        "executor_started": True,
                    },
                    event_id="sandbox-started:" + invocation.call_id,
                )
                started_recorded = True
            if result["status"] not in {"finished", "unknown"}:
                await asyncio.sleep(0.2)
    except (ServiceError, asyncio.CancelledError) as error:
        interrupted = error
        if (
            accepted.get("new")
            and result is None
            and isinstance(error, ServiceError)
            and error.code
            in {
                "capacity",
                "workspace_busy",
                "invalid_input",
                "input_revision_conflict",
                "permission_denied",
            }
        ):
            submission_attempted = False

        async def stop_original() -> dict[str, Any]:
            async with asyncio.timeout(18):
                while True:
                    stopped = await broker.call("command_status", **args, renew=False, cancel=True)
                    if stopped["status"] in {"finished", "unknown"}:
                        return stopped
                    await asyncio.sleep(0.2)

        if not submission_attempted:
            result = {
                "status": "finished",
                "generation": values.expected_generation,
                "receipt": {
                    "executor_started": False,
                    "exit_code": None,
                    "stdout": "",
                    "stderr": "",
                    "output_complete": True,
                    "cleanup": "clean",
                    "code": error.code if isinstance(error, ServiceError) else "cancelled",
                },
            }
        else:
            try:
                result = await settled(stop_original())
            except (ServiceError, TimeoutError):
                result = None
    receipt = result.get("receipt") if result else None
    if not isinstance(receipt, dict):
        receipt = {
            "executor_started": None,
            "exit_code": None,
            "stdout": "",
            "stderr": "",
            "output_complete": False,
            "cleanup": "unknown",
            "code": "execution_unknown",
        }
    if receipt.get("executor_started") and not started_recorded:
        await settled(
            service.store.append(
                invocation.run_id,
                "sandbox_started",
                {
                    "call_id": invocation.call_id,
                    "project_id": values.project_id,
                    "operation_id": operation_id,
                    "executor_started": True,
                },
                event_id="sandbox-started:" + invocation.call_id,
            )
        )
    generation = (
        result.get("generation", values.expected_generation + 1)
        if result
        else values.expected_generation + 1
    )
    receipt = {
        **receipt,
        "project_id": values.project_id,
        "operation_id": operation_id,
        "generation": generation,
        "verification": "not_evaluated",
    }

    async def finish(conn: AsyncConnection) -> None:
        # 授权撤销不抹除已发生的文件变动与停止回执；该事务只结算原意图。
        row = (
            (await conn.execute(select(projects).where(projects.c.project_id == values.project_id)))
            .mappings()
            .one()
        )
        if row["lease_id"] != operation_id:
            raise ServiceError("sandbox_cleanup_required")
        clean = (
            receipt["cleanup"] == "clean" and result is not None and result["status"] == "finished"
        )
        await conn.execute(
            update(projects)
            .where(projects.c.project_id == values.project_id)
            .values(
                generation=generation,
                status="ready" if clean else "blocked",
                lease_id=None if clean else operation_id,
                lease_expires_at=None,
                updated_at=time.time(),
            )
        )
        await conn.execute(
            update(operations)
            .where(operations.c.operation_id == operation_id)
            .values(
                status="completed" if clean else "unknown",
                result={**intent, "receipt": receipt},
                updated_at=time.time(),
            )
        )

    await settled(service.store._write(finish))
    if interrupted is not None:
        raise interrupted
    scope.check()
    return receipt


async def observe_command(
    service: SandboxService, invocation: Invocation
) -> dict[str, JsonValue] | None:
    async with service.store.lock, service.store.engine.connect() as conn:
        row = (
            (
                await conn.execute(
                    select(operations).where(
                        operations.c.request_key == request_key(invocation),
                        operations.c.kind == "command",
                    )
                )
            )
            .mappings()
            .first()
        )
    if row is None or row["result"].get("run_id") != invocation.run_id:
        return None
    return cast(dict[str, JsonValue] | None, row["result"].get("receipt"))
