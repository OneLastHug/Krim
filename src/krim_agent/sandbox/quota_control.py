"""在线配额控制面：目标配置与已应用配置分开保存，状态查询只推进原操作。"""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError, fingerprint
from krim_agent.runtime.contracts import settled
from krim_agent.sandbox.management import key_check
from krim_agent.sandbox.models import ConfigureQuotaBody
from krim_agent.security import PrincipalContext
from krim_agent.storage.sandbox_models import sandbox_environments as environments
from krim_agent.storage.sandbox_models import sandbox_operations as operations
from krim_agent.storage.sandbox_models import sandbox_quota_state as states

if TYPE_CHECKING:
    from krim_agent.sandbox.service import SandboxService


async def preview(
    service: SandboxService, actor: PrincipalContext, principal_id: str, body: object
) -> dict[str, Any]:
    values = service._parse(ConfigureQuotaBody, body)
    try:
        limits = service.configuration.quotas.resolve(values.profile_name, values.overrides)
    except ValueError:
        raise ServiceError("sandbox_config_invalid", 422) from None
    if service.broker is None:
        raise ServiceError("sandbox_backend_unavailable", 503)

    async def read(conn: AsyncConnection) -> Any:
        row = (
            (
                await conn.execute(
                    select(environments).where(environments.c.principal_id == principal_id)
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise ServiceError("not_found", 404)
        if row["revision"] != values.expected_revision:
            raise ServiceError("revision_conflict")
        return dict(row)

    env = await service._read(actor, read, admin=True)
    response = await service.broker.call(
        "quota_preview", environment_id=env["environment_id"], limits=limits.model_dump(mode="json")
    )
    await service._read(actor, read, admin=True)
    metadata = {
        "environment_id": env["environment_id"],
        "principal_id": principal_id,
        "configuration": values.model_dump(mode="json", exclude_unset=True),
        "limits": limits.model_dump(mode="json"),
        "response": response,
    }

    async def save(conn: AsyncConnection) -> None:
        await service._authorize(conn, actor, admin=True)
        now = time.time()
        await conn.execute(
            insert(operations).values(
                operation_id=str(uuid.uuid4()),
                principal_id=actor.principal_id,
                request_key="quota-plan:" + response["plan_id"],
                request_hash=fingerprint(metadata),
                project_id=None,
                kind="quota_plan",
                status="completed",
                result=metadata,
                created_at=now,
                updated_at=now,
            )
        )

    await settled(service.store._write(save))
    return {**response, "expected_revision": env["revision"]}


async def confirm(
    service: SandboxService,
    actor: PrincipalContext,
    principal_id: str,
    plan_id: str,
    digest: str,
    expected_revision: int,
    request_key: str,
) -> dict[str, Any]:
    key_check(request_key)
    if type(expected_revision) is not int:
        raise ServiceError("invalid_input", 422)
    if service.broker is None:
        raise ServiceError("sandbox_backend_unavailable", 503)
    payload = {
        "target": principal_id,
        "plan": plan_id,
        "digest": digest,
        "revision": expected_revision,
    }

    async def reserve(conn: AsyncConnection) -> dict[str, Any]:
        await service._authorize(conn, actor, admin=True)
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
            if old["request_hash"] != fingerprint(payload):
                raise ServiceError("key_conflict")
            return dict(old)
        plan = (
            (
                await conn.execute(
                    select(operations).where(
                        operations.c.principal_id == actor.principal_id,
                        operations.c.request_key == "quota-plan:" + plan_id,
                        operations.c.kind == "quota_plan",
                    )
                )
            )
            .mappings()
            .first()
        )
        if plan is None or plan["result"]["principal_id"] != principal_id:
            raise ServiceError("not_found", 404)
        accepted = plan["result"]
        if accepted["response"]["plan_digest"] != digest:
            raise ServiceError("key_conflict")
        if accepted["response"]["expires_at"] < time.time():
            raise ServiceError("approval_expired")
        env = (
            (
                await conn.execute(
                    select(environments).where(environments.c.principal_id == principal_id)
                )
            )
            .mappings()
            .one()
        )
        if (
            env["revision"] != expected_revision
            or accepted["configuration"]["expected_revision"] != expected_revision
        ):
            raise ServiceError("revision_conflict")
        prior = (
            (await conn.execute(select(states).where(states.c.principal_id == principal_id)))
            .mappings()
            .first()
        )
        if prior is not None and prior["state"] not in {
            "applied",
            "cancelled",
            "failed_validation",
        }:
            raise ServiceError("workspace_busy")
        operation = str(uuid.uuid4())
        now = time.time()
        state = {
            "principal_id": principal_id,
            "environment_id": env["environment_id"],
            "desired": accepted,
            "old_desired": {
                "profile_name": env["profile_name"],
                "quota_override": env["quota_override"],
            },
            "desired_revision": expected_revision + 1,
            "applied_revision": prior["applied_revision"]
            if prior is not None
            else expected_revision,
            "operation_id": operation,
            "state": "applying",
            "updated_at": now,
        }
        if prior is None:
            await conn.execute(insert(states).values(**state))
        else:
            await conn.execute(
                update(states).where(states.c.principal_id == principal_id).values(**state)
            )
        await conn.execute(
            update(environments)
            .where(environments.c.principal_id == principal_id)
            .values(revision=expected_revision + 1, updated_at=now)
        )
        intent = {
            **payload,
            "environment_id": env["environment_id"],
            "plan_id": plan_id,
            "plan_digest": digest,
            "retirement": bool(accepted["response"].get("retirement")),
        }
        await conn.execute(
            insert(operations).values(
                operation_id=operation,
                principal_id=actor.principal_id,
                request_key=request_key,
                request_hash=fingerprint(payload),
                project_id=None,
                kind="quota_confirm",
                status="pending",
                result=intent,
                created_at=now,
                updated_at=now,
            )
        )
        return {"operation_id": operation, "result": intent, "status": "pending", "new": True}

    row = await settled(service.store._write(reserve))
    if row["status"] == "completed":
        return cast(dict[str, Any], row["result"]["response"])
    if row["status"] == "failed":
        raise ServiceError(row["result"]["error"], 409)
    try:
        result = await service.broker.call(
            "quota_retirement_confirm" if row["result"].get("retirement") else "quota_confirm",
            environment_id=row["result"]["environment_id"],
            plan_id=plan_id,
            plan_digest=digest,
            operation_id=row["operation_id"],
        )
    except ServiceError as error:
        if error.code in {"sandbox_limit", "invalid_input", "approval_expired", "key_conflict"}:

            async def failed(conn: AsyncConnection, reason: str = error.code) -> None:
                await conn.execute(
                    update(states)
                    .where(
                        states.c.principal_id == principal_id,
                        states.c.operation_id == row["operation_id"],
                    )
                    .values(state="failed_validation")
                )
                await conn.execute(
                    update(operations)
                    .where(operations.c.operation_id == row["operation_id"])
                    .values(status="failed", result={**row["result"], "error": reason})
                )

            await settled(service.store._write(failed))
        raise

    return await settle(service, actor, principal_id, row["operation_id"], result)


async def status(
    service: SandboxService,
    actor: PrincipalContext,
    principal_id: str,
    operation_id: str,
    *,
    cancel: bool = False,
) -> dict[str, Any]:
    if service.broker is None:
        raise ServiceError("sandbox_backend_unavailable", 503)

    async def read(conn: AsyncConnection) -> Any:
        state = (
            (
                await conn.execute(
                    select(states).where(
                        states.c.principal_id == principal_id, states.c.operation_id == operation_id
                    )
                )
            )
            .mappings()
            .first()
        )
        if state is None:
            history = (
                (
                    await conn.execute(
                        select(operations).where(
                            operations.c.operation_id == operation_id,
                            operations.c.kind == "quota_confirm",
                        )
                    )
                )
                .mappings()
                .first()
            )
            if (
                history is None
                or history["result"].get("target") != principal_id
                or history["status"] != "completed"
                or cancel
            ):
                raise ServiceError("not_found", 404)
            return {"historical": history["result"]["response"]}
        return dict(state)

    row = await service._read(actor, read, admin=True)
    if "historical" in row:
        return cast(dict[str, Any], row["historical"])
    prefix = "quota_retirement_" if row["desired"]["response"].get("retirement") else "quota_"
    result = await service.broker.call(
        prefix + ("cancel" if cancel else "status"),
        environment_id=row["environment_id"],
        operation_id=operation_id,
    )
    return await settle(service, actor, principal_id, operation_id, result)


async def settle(
    service: SandboxService,
    actor: PrincipalContext,
    principal_id: str,
    operation_id: str,
    result: dict[str, Any],
) -> dict[str, Any]:
    async def write(conn: AsyncConnection) -> dict[str, Any]:
        state = (
            (
                await conn.execute(
                    select(states).where(
                        states.c.principal_id == principal_id, states.c.operation_id == operation_id
                    )
                )
            )
            .mappings()
            .one()
        )
        phase = result["quota_state"]
        if phase == "applied":
            desired = state["desired"]["configuration"]
            await conn.execute(
                update(environments)
                .where(environments.c.principal_id == principal_id)
                .values(
                    profile_name=desired.get("profile_name"),
                    quota_override=desired.get("overrides", {}),
                    updated_at=time.time(),
                )
            )
        await conn.execute(
            update(states)
            .where(states.c.principal_id == principal_id)
            .values(
                state=phase,
                applied_revision=state["desired_revision"]
                if phase == "applied"
                else state["applied_revision"],
                updated_at=time.time(),
            )
        )
        original = (
            (
                await conn.execute(
                    select(operations).where(operations.c.operation_id == operation_id)
                )
            )
            .mappings()
            .one()
        )
        response = {
            **result,
            "desired_revision": state["desired_revision"],
            "applied_revision": state["desired_revision"]
            if phase == "applied"
            else state["applied_revision"],
        }
        await conn.execute(
            update(operations)
            .where(operations.c.operation_id == operation_id)
            .values(
                status="completed" if phase in {"applied", "cancelled"} else "pending",
                result={**original["result"], "response": response},
                updated_at=time.time(),
            )
        )
        return {
            **result,
            "desired_revision": state["desired_revision"],
            "applied_revision": state["desired_revision"]
            if phase == "applied"
            else state["applied_revision"],
        }

    response = await settled(service.store._write(write))

    async def permitted(conn: AsyncConnection) -> None:
        await service._authorize(conn, actor, admin=True)

    await service._read(actor, permitted, admin=True)
    return response
