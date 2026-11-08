"""恢复/GC控制面：先持久意图，Broker按原ID核实，结算不掩盖已发生修改。"""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.runtime.contracts import settled
from krim_agent.security import PrincipalContext
from krim_agent.storage.sandbox_models import sandbox_operations as operations
from krim_agent.storage.sandbox_models import sandbox_projects as projects

if TYPE_CHECKING:
    from krim_agent.sandbox.service import SandboxService

WRITE_ACTIONS = {
    "restore_confirm",
    "restore_resume",
    "restore_abandon",
    "restore_reconcile",
    "checkpoint_retention",
    "checkpoint_cleanup_confirm",
}
FILE_ACTIONS = {"restore_confirm", "restore_resume", "restore_abandon", "restore_reconcile"}
TERMINAL = {"completed", "abandoned_partial", "conflict", "failed_preparation"}


def key_check(key: str) -> None:
    if (
        type(key) is not str
        or not 1 <= len(key) <= 128
        or any(not 33 <= ord(c) <= 126 for c in key)
    ):
        raise ServiceError("invalid_input", 422)


async def project_action(
    service: SandboxService,
    actor: PrincipalContext,
    project_id: str,
    action: str,
    values: dict[str, Any],
    *,
    request_key: str | None = None,
    expected_generation: int | None = None,
) -> dict[str, Any]:
    broker = service.broker
    if broker is None:
        raise ServiceError("sandbox_backend_unavailable", 503)
    write = action in WRITE_ACTIONS
    # 预览也要具备写权限，但不产生任何用户文件修改。
    project = await service.project(actor, project_id, write=write or action == "restore_preview")
    params = {**values, "environment_id": project["environment_id"], "project_id": project_id}
    if not write:
        result = await broker.call(action, **params)
        await service.project(actor, project_id)
        return result
    if request_key is None:
        raise ServiceError("invalid_input", 422)
    key_check(request_key)
    if type(expected_generation) is not int or expected_generation < 0:
        raise ServiceError("invalid_input", 422)
    digest = fingerprint(
        {
            "action": action,
            "project_id": project_id,
            "params": values,
            "expected_generation": expected_generation,
        }
    )

    async def reserve(conn: AsyncConnection) -> dict[str, Any]:
        await service._authorize(conn, actor)
        if action in {"restore_confirm", "restore_resume"}:
            from krim_agent.storage.sandbox_models import sandbox_quota_state

            pending_quota = (
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
            if pending_quota is not None and pending_quota["state"] not in {
                "applied",
                "cancelled",
                "failed_validation",
            }:
                raise ServiceError("workspace_busy")
        row = (
            (
                await conn.execute(
                    select(projects).where(
                        projects.c.project_id == project_id,
                        projects.c.principal_id == actor.principal_id,
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise ServiceError("not_found", 404)
        actor.member(row["workspace_id"], write=True)
        old = (
            (
                await conn.execute(
                    select(operations).where(
                        operations.c.principal_id == actor.principal_id,
                        operations.c.request_key == request_key,
                    )
                )
            )
            .mappings()
            .first()
        )
        if old is not None:
            if old["request_hash"] != digest:
                raise ServiceError("key_conflict")
            return dict(old)
        if row["generation"] != expected_generation:
            raise ServiceError("input_revision_conflict")
        operation_id = str(uuid.uuid4())
        arguments = dict(params)
        if action == "restore_confirm":
            if row["lease_id"] is not None or row["status"] != "ready":
                raise ServiceError("workspace_busy")
            arguments["operation_id"] = operation_id
            arguments["generation"] = expected_generation
        elif action in FILE_ACTIONS:
            original = (
                (
                    await conn.execute(
                        select(operations).where(
                            operations.c.operation_id == arguments["operation_id"],
                            operations.c.project_id == project_id,
                            operations.c.principal_id == actor.principal_id,
                            operations.c.kind == "restore_confirm",
                        )
                    )
                )
                .mappings()
                .first()
            )
            if original is None:
                raise ServiceError("not_found", 404)
            if row["lease_id"] not in {None, arguments["operation_id"]}:
                raise ServiceError("workspace_busy")
        if action in FILE_ACTIONS:
            await conn.execute(
                update(projects)
                .where(projects.c.project_id == project_id)
                .values(
                    status="restoring", lease_id=arguments["operation_id"], lease_expires_at=None
                )
            )
        if action == "checkpoint_cleanup_confirm":
            arguments["request_key"] = request_key
        now = time.time()
        if action == "restore_resume":
            arguments["action_id"] = operation_id
        intent = {
            "project_id": project_id,
            "action": action,
            "arguments": arguments,
            "expected_generation": expected_generation,
            "prior_project_status": row["status"],
            "prior_lease_id": row["lease_id"],
        }
        await conn.execute(
            insert(operations).values(
                operation_id=operation_id,
                principal_id=actor.principal_id,
                request_key=request_key,
                request_hash=digest,
                project_id=project_id,
                kind=action,
                status="pending",
                result=intent,
                created_at=now,
                updated_at=now,
            )
        )
        return {"operation_id": operation_id, "status": "pending", "result": intent, "new": True}

    record = await settled(service.store._write(reserve))
    if record["status"] == "completed":
        return cast(dict[str, Any], record["result"]["response"])
    if record["status"] == "failed":
        raise ServiceError(record["result"]["error"])
    arguments = record["result"]["arguments"]
    service._live(actor)
    try:
        result = await broker.call(action, **arguments)
    except ServiceError as error:
        if action == "restore_confirm" and error.code in {
            "invalid_input",
            "artifact_limit",
            "key_conflict",
            "revision_conflict",
            "input_revision_conflict",
            "approval_expired",
            "not_found",
        }:

            async def reject(conn: AsyncConnection, reason: str = error.code) -> None:
                current = (
                    (
                        await conn.execute(
                            select(projects).where(projects.c.project_id == project_id)
                        )
                    )
                    .mappings()
                    .one()
                )
                if current["lease_id"] == arguments["operation_id"]:
                    await conn.execute(
                        update(projects)
                        .where(projects.c.project_id == project_id)
                        .values(
                            status=record["result"]["prior_project_status"],
                            lease_id=record["result"]["prior_lease_id"],
                            lease_expires_at=None,
                        )
                    )
                await conn.execute(
                    update(operations)
                    .where(operations.c.operation_id == record["operation_id"])
                    .values(status="failed", result={**record["result"], "error": reason})
                )

            await settled(service.store._write(reject))
        raise

    if record.get("new") and action in {"restore_confirm", "restore_resume"}:
        deadline = time.monotonic() + 120
        for index in range(7):
            if (
                result.get("status") != "paused"
                or result.get("reason") != "batch_limit"
                or time.monotonic() >= deadline
            ):
                break
            try:
                # 每次下一文件之前重新取当前权限，而非持一份旧授权跑完整目录。
                service._live(actor)
                await service.project(actor, project_id, write=True)
            except ServiceError:
                break
            continued = {
                "environment_id": arguments["environment_id"],
                "project_id": project_id,
                "operation_id": arguments["operation_id"],
                "action_id": str(uuid.uuid5(uuid.UUID(record["operation_id"]), str(index))),
            }
            result = await broker.call("restore_resume", **continued)

    async def finish(conn: AsyncConnection) -> None:
        # Broker完成文件步骤后，即使主体撤权也要登记原结果；之后不向失效身份返回内容。
        if action in FILE_ACTIONS:
            row = (
                (await conn.execute(select(projects).where(projects.c.project_id == project_id)))
                .mappings()
                .one()
            )
            if row["lease_id"] not in {None, arguments["operation_id"]}:
                raise ServiceError("sandbox_cleanup_required")
            status = result["status"]
            clean = status in TERMINAL
            await conn.execute(
                update(projects)
                .where(projects.c.project_id == project_id)
                .values(
                    generation=max(row["generation"], result["generation"]),
                    status="ready" if clean else "blocked",
                    lease_id=None if clean else arguments["operation_id"],
                    lease_expires_at=None,
                    updated_at=time.time(),
                )
            )
        await conn.execute(
            update(operations)
            .where(operations.c.operation_id == record["operation_id"])
            .values(
                status="completed",
                result={**record["result"], "response": result},
                updated_at=time.time(),
            )
        )

    await settled(service.store._write(finish))
    service._live(actor)
    await service.project(actor, project_id)
    return result
