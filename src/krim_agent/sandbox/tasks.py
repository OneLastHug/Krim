"""Bounded run-scoped background sandbox jobs and completion evidence."""

from __future__ import annotations

import asyncio
import base64
import time
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal, cast

from pydantic import Field, JsonValue, model_validator
from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError, StrictModel, fingerprint
from krim_agent.runtime.contracts import settled
from krim_agent.sandbox.command import SandboxCommand
from krim_agent.sandbox.xfs import internal_id
from krim_agent.security import PrincipalContext
from krim_agent.storage.models import runs
from krim_agent.storage.sandbox_models import (
    sandbox_operations,
    sandbox_projects,
    sandbox_quota_state,
)
from krim_agent.workspace.types import CompletionCondition, path_parts

if TYPE_CHECKING:
    from krim_agent.sandbox.operations import SessionCommandScope
    from krim_agent.sandbox.service import SandboxService
    from krim_agent.tools.core import Invocation


class TaskStart(SandboxCommand):
    """An approved command whose execution has a fixed, non-renewable watchdog."""


class TaskReference(StrictModel):
    project_id: str | None = None
    operation_id: str

    @model_validator(mode="after")
    def valid_ids(self) -> TaskReference:
        if self.project_id is not None:
            internal_id(self.project_id)
        internal_id(self.operation_id)
        return self


class TaskWait(TaskReference):
    seconds: int = Field(default=5, ge=1, le=10)


class TaskCancel(TaskReference):
    expected_generation: int = Field(ge=0)


class TaskLog(TaskReference):
    max_bytes: int = Field(default=4096, ge=1, le=8192)


class SandboxCompletion(StrictModel):
    """Human-authored checks applied after run-scoped tasks finish."""

    require_exit_zero: bool = True
    minimum_tasks: int = Field(default=1, ge=1, le=16)
    required_paths: tuple[str, ...] = Field(default=(), max_length=16)
    conditions: tuple[CompletionCondition, ...] = Field(default=(), max_length=16)

    @model_validator(mode="after")
    def bounded(self) -> SandboxCompletion:
        if len(self.required_paths) + len(self.conditions) > 16:
            raise ValueError("完成条件过多")
        for path in self.required_paths:
            path_parts(path)
        if len(set(self.required_paths)) != len(self.required_paths):
            raise ValueError("重复路径")
        if len({condition.id for condition in self.conditions}) != len(self.conditions):
            raise ValueError("重复条件")
        return self


def _task_status(record: dict[str, Any]) -> str:
    status = record.get("status")
    if status == "finished":
        return "finished"
    if status == "unknown" or status == "reconciling":
        return "unknown"
    if record.get("executor_started") is True or status in {
        "creating", "created", "gate_verified", "started", "output"
    }:
        return "running"
    return "queued"


def _receipt(record: dict[str, Any], operation_id: str, project_id: str) -> dict[str, Any]:
    receipt = record.get("receipt")
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
    generation = record.get("generation")
    return {
        **receipt,
        "project_id": project_id,
        "operation_id": operation_id,
        "generation": generation,
        "verification": "not_evaluated",
    }


async def start_task(
    service: SandboxService,
    actor: PrincipalContext,
    values: TaskStart,
    invocation: Invocation,
    scope: SessionCommandScope,
) -> dict[str, Any]:
    """Persist and submit exactly one run-scoped job; never wait or renew its permit."""
    from krim_agent.tools.pipeline import CURRENT

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
    selected_runtime = service.configuration.resolve_runtime(values.program)
    expected_runtime = {
        "image": selected_runtime.image if selected_runtime is not None
        else service.configuration.command_image,
        "fingerprint": selected_runtime.fingerprint if selected_runtime is not None else None,
    }
    digest = fingerprint({
        "run": invocation.run_id,
        "call": invocation.call_id,
        "tool_hash": invocation.tool_hash,
        "command": payload,
        "runtime": expected_runtime,
        "background": True,
    })
    key = "command:" + fingerprint({"run": invocation.run_id, "call": invocation.call_id})

    async def reserve(conn: AsyncConnection) -> dict[str, Any]:
        await service._authorize(conn, actor)
        scope.check()
        from krim_agent.sandbox.bindings import binding_row

        quota = (await conn.execute(select(sandbox_quota_state).where(
            sandbox_quota_state.c.principal_id == actor.principal_id
        ))).mappings().first()
        if quota is not None and quota["state"] not in {
            "applied", "cancelled", "failed_validation"
        }:
            raise ServiceError("workspace_busy")
        binding = await binding_row(conn, scope.session_id)
        if (
            (binding["revision"] if binding else 0) != scope.binding_revision
            or (binding["project_id"] if binding else None) != scope.project_id
            or scope.project_id is not None and values.project_id != scope.project_id
        ):
            raise ServiceError("target_binding_changed")
        old = (await conn.execute(select(sandbox_operations).where(
            sandbox_operations.c.principal_id == actor.principal_id,
            sandbox_operations.c.request_key == key,
        ))).mappings().first()
        if old is not None:
            if old["request_hash"] != digest:
                raise ServiceError("key_conflict")
            if old["result"].get("authorization_generation") != actor.generation:
                raise ServiceError("permission_denied")
            return dict(old)
        project = (await conn.execute(select(sandbox_projects).where(
            sandbox_projects.c.project_id == values.project_id,
            sandbox_projects.c.principal_id == actor.principal_id,
        ))).mappings().first()
        if project is None:
            raise ServiceError("not_found", 404)
        if project["workspace_id"] != scope.workspace_id:
            raise ServiceError("permission_denied")
        actor.member(project["workspace_id"], write=True)
        if project["generation"] != values.expected_generation:
            raise ServiceError("input_revision_conflict")
        if project["status"] != "ready" or project["lease_id"] is not None:
            raise ServiceError("workspace_busy")
        run_status = await conn.scalar(select(runs.c.status).where(runs.c.id == invocation.run_id))
        if run_status != "running":
            raise ServiceError("run_closed")
        operation_id = str(uuid.uuid4())
        intent = {
            "project_id": values.project_id,
            "environment_id": project["environment_id"],
            "run_id": invocation.run_id,
            "session_id": scope.session_id,
            "call_id": invocation.call_id,
            "tool_hash": invocation.tool_hash,
            "command": payload,
            "authorization_generation": actor.generation,
            "image": expected_runtime["image"],
            "runtime_fingerprint": expected_runtime["fingerprint"],
            "program": values.program,
            "background": True,
            "verification": "not_evaluated",
        }
        now = time.time()
        await conn.execute(insert(sandbox_operations).values(
            operation_id=operation_id,
            principal_id=actor.principal_id,
            request_key=key,
            request_hash=digest,
            project_id=values.project_id,
            kind="command",
            status="intent",
            result=intent,
            created_at=now,
            updated_at=now,
        ))
        await conn.execute(update(sandbox_projects).where(
            sandbox_projects.c.project_id == values.project_id
        ).values(status="running", lease_id=operation_id, lease_expires_at=now + 120))
        return {"operation_id": operation_id, "project_id": values.project_id,
                "status": "intent", "result": intent}

    operation = await settled(service.store._write(reserve))
    operation_id = operation["operation_id"]
    if operation["status"] in {"completed", "unknown"}:
        return {"operation_id": operation_id, "status": "accepted"}
    args = {
        "environment_id": operation["result"]["environment_id"],
        "project_id": values.project_id,
        "operation_id": operation_id,
    }
    try:
        health = await broker.call("health")
        runtime_health = health.get("runtimes", {}).get(values.program, {})
        if not isinstance(runtime_health, dict):
            runtime_health = {}
        image = runtime_health.get("image")
        if image is None and values.program == "python":
            image = health.get("image")
        if (
            values.program not in health.get("configured_programs", ["python"])
            or image != operation["result"]["image"]
            or operation["result"]["runtime_fingerprint"] is not None
            and runtime_health.get("fingerprint") != operation["result"]["runtime_fingerprint"]
        ):
            raise ServiceError("target_binding_changed")
        scope.check()
        await broker.call(
            "command_start_background",
            **args,
            command=payload,
            runtime_image=operation["result"]["image"],
            runtime_fingerprint=operation["result"]["runtime_fingerprint"],
        )
        scope.check()
    except asyncio.CancelledError:
        async def settle_cancelled() -> None:
            try:
                result = await broker.call(
                    "command_status", **args, renew=False, cancel=True
                )
                if result.get("status") in {"finished", "unknown"}:
                    await _settle(service, operation, result)
                    return
                deadline = time.monotonic() + 18
                while time.monotonic() < deadline:
                    await asyncio.sleep(0.2)
                    result = await broker.call(
                        "command_status", **args, renew=False, cancel=True
                    )
                    if result.get("status") in {"finished", "unknown"}:
                        await _settle(service, operation, result)
                        return
            except (ServiceError, TimeoutError):
                return

        await settled(settle_cancelled())
        raise
    except ServiceError as error:
        if error.code in {
            "capacity", "workspace_busy", "invalid_input", "input_revision_conflict",
            "permission_denied", "target_binding_changed", "sandbox_backend_unavailable",
        }:
            failure_code = error.code

            async def reject(conn: AsyncConnection) -> None:
                project = (await conn.execute(select(sandbox_projects).where(
                    sandbox_projects.c.project_id == values.project_id
                ))).mappings().one()
                current = (await conn.execute(select(sandbox_operations).where(
                    sandbox_operations.c.operation_id == operation_id
                ))).mappings().one()
                prior = current["result"].get("receipt")
                receipt = prior if isinstance(prior, dict) else {
                    "executor_started": False,
                    "exit_code": None,
                    "stdout": "",
                    "stderr": "",
                    "output_complete": True,
                    "cleanup": "clean",
                    "code": failure_code,
                    "generation": project["generation"],
                    "verification": "not_evaluated",
                }
                now = time.time()
                if project["lease_id"] == operation_id:
                    await conn.execute(update(sandbox_projects).where(
                        sandbox_projects.c.project_id == values.project_id
                    ).values(
                        status="ready", lease_id=None, lease_expires_at=None, updated_at=now
                    ))
                await conn.execute(update(sandbox_operations).where(
                    sandbox_operations.c.operation_id == operation_id
                ).values(
                    status="completed",
                    result={**current["result"], "receipt": receipt},
                    updated_at=now,
                ))

            await settled(service.store._write(reject))
            raise
        # An ambiguous submit is treated as an existing job and immediately stopped.
        try:
            result = await broker.call("command_status", **args, renew=False, cancel=True)
            if result.get("status") not in {"finished", "unknown"}:
                raise ServiceError("sandbox_operation_unknown")
            await _settle(service, operation, result)
        except (ServiceError, TimeoutError):
            pass
        raise
    await service.store.append(
        invocation.run_id,
        "sandbox_task_started",
        {"operation_id": operation_id, "project_id": values.project_id,
         "status": "accepted", "permit_seconds": values.wall_seconds + 45},
        event_id="sandbox-task-started:" + operation_id,
    )
    return {"operation_id": operation_id, "project_id": values.project_id, "status": "accepted"}


async def _operation(
    service: SandboxService, actor: PrincipalContext, operation_id: str, session_id: str
) -> dict[str, Any]:
    async def read(conn: AsyncConnection) -> dict[str, Any]:
        row = (
            await conn.execute(
                select(sandbox_operations).where(
                    sandbox_operations.c.operation_id == operation_id,
                    sandbox_operations.c.principal_id == actor.principal_id,
                    sandbox_operations.c.kind == "command",
                )
            )
        ).mappings().first()
        if row is None or row["result"].get("background") is not True:
            raise ServiceError("not_found", 404)
        if row["result"].get("session_id") != session_id:
            raise ServiceError("permission_denied")
        return dict(row)

    return await service._read(actor, read)


async def _settle(
    service: SandboxService, operation: dict[str, Any], record: dict[str, Any]
) -> dict[str, Any]:
    operation_id = operation["operation_id"]
    project_id = operation["project_id"]
    receipt = _receipt(record, operation_id, project_id)
    generation = record.get("generation")
    if type(generation) is not int:
        generation = int(operation["result"]["command"]["expected_generation"]) + 1
        receipt["generation"] = generation
    clean = record.get("status") == "finished" and receipt["cleanup"] == "clean"

    async def finish(conn: AsyncConnection) -> dict[str, Any]:
        current = (
            await conn.execute(
                select(sandbox_operations).where(
                    sandbox_operations.c.operation_id == operation_id
                )
            )
        ).mappings().one()
        if current["status"] in {"completed", "unknown"}:
            old_receipt = current["result"].get("receipt")
            return old_receipt if isinstance(old_receipt, dict) else receipt
        project = (
            await conn.execute(
                select(sandbox_projects).where(sandbox_projects.c.project_id == project_id)
            )
        ).mappings().one()
        if project["lease_id"] != operation_id:
            raise ServiceError("sandbox_cleanup_required")
        now = time.time()
        await conn.execute(
            update(sandbox_projects)
            .where(sandbox_projects.c.project_id == project_id)
            .values(
                generation=generation,
                status="ready" if clean else "blocked",
                lease_id=None if clean else operation_id,
                lease_expires_at=None,
                updated_at=now,
            )
        )
        await conn.execute(
            update(sandbox_operations)
            .where(sandbox_operations.c.operation_id == operation_id)
            .values(
                status="completed" if clean else "unknown",
                result={**current["result"], "receipt": receipt},
                updated_at=now,
            )
        )
        return receipt

    return await service.store._write(finish)


async def _observe(
    service: SandboxService,
    actor: PrincipalContext,
    operation: dict[str, Any],
    session_id: str,
    run_id: str,
    check: Callable[[], None],
    *,
    cancel: bool = False,
) -> tuple[str, dict[str, Any] | None, dict[str, Any]]:
    check()
    project_id = operation["project_id"]
    project = await service._project_state(actor, project_id, write=cancel)
    broker = service.broker
    if broker is None:
        raise ServiceError("sandbox_backend_unavailable", 503)
    async def observe() -> dict[str, Any]:
        return await broker.call(
            "command_status",
            environment_id=project["environment_id"],
            project_id=project_id,
            operation_id=operation["operation_id"],
            renew=False,
            cancel=cancel,
        )

    result = await settled(observe())
    check()
    if operation["status"] in {"completed", "unknown"}:
        result["status"] = "finished" if operation["status"] == "completed" else "unknown"
    state = _task_status(result)
    receipt: dict[str, Any] | None = None
    if result.get("status") in {"finished", "unknown"}:
        if operation["status"] not in {"completed", "unknown"}:
            receipt = await _settle(service, operation, result)
            operation = await _operation(service, actor, operation["operation_id"], session_id)
        else:
            stored = operation["result"].get("receipt")
            receipt = (
                stored
                if isinstance(stored, dict)
                else _receipt(result, operation["operation_id"], project_id)
            )
        if operation["result"].get("finished_event_emitted") is not True:
            await service.store.append(
                run_id,
                "sandbox_task_finished",
                {
                    "operation_id": operation["operation_id"],
                    "project_id": project_id,
                    "task_status": state,
                    "exit_code": receipt.get("exit_code"),
                    "code": receipt.get("code"),
                    "verification": receipt.get("verification", "not_evaluated"),
                },
                event_id="sandbox-task-finished:" + operation["operation_id"],
            )
    check()
    return state, receipt, result


def _view(
    operation: dict[str, Any], state: str, receipt: dict[str, Any] | None
) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {
        "operation_id": operation["operation_id"],
        "project_id": operation["project_id"],
        "status": state,
        "verification": (
            receipt.get("verification", "not_evaluated") if receipt else "not_evaluated"
        ),
        "executor_started": receipt.get("executor_started") if receipt else None,
        "exit_code": receipt.get("exit_code") if receipt else None,
        "code": receipt.get("code") if receipt else None,
        "output_complete": receipt.get("output_complete") if receipt else None,
        "generation": receipt.get("generation") if receipt else None,
    }
    return result


async def status(
    service: SandboxService,
    scope: SessionCommandScope,
    values: TaskReference,
    invocation: Invocation,
) -> dict[str, JsonValue]:
    operation = await _operation(service, scope.actor, values.operation_id, scope.session_id)
    if operation["project_id"] != values.project_id:
        raise ServiceError("permission_denied")
    state, receipt, _record = await _observe(
        service, scope.actor, operation, scope.session_id, invocation.run_id, scope.check
    )
    return _view(operation, state, receipt)


async def log(
    service: SandboxService, scope: SessionCommandScope, values: TaskLog, invocation: Invocation
) -> dict[str, JsonValue]:
    operation = await _operation(service, scope.actor, values.operation_id, scope.session_id)
    if operation["project_id"] != values.project_id:
        raise ServiceError("permission_denied")
    state, receipt, record = await _observe(
        service, scope.actor, operation, scope.session_id, invocation.run_id, scope.check
    )
    if receipt is None:
        project = await service._project_state(scope.actor, operation["project_id"])
        broker = service.broker
        if broker is None:
            raise ServiceError("sandbox_backend_unavailable", 503)
        logs = await broker.call(
            "command_logs",
            environment_id=project["environment_id"],
            project_id=operation["project_id"],
            operation_id=operation["operation_id"],
            max_bytes=values.max_bytes,
        )
        record = {**record, "logs": logs}
    source = receipt if receipt is not None else record.get("logs", {})
    if not isinstance(source, dict):
        source = {}
    output: dict[str, JsonValue] = _view(operation, state, receipt)
    for name in ("stdout", "stderr"):
        value = source.get(name, "")
        if isinstance(value, str):
            try:
                data = base64.b64decode(value, validate=True)
            except ValueError:
                data = b""
            output[name] = base64.b64encode(data[-values.max_bytes :]).decode()
        else:
            output[name] = ""
    return output


async def wait(
    service: SandboxService, scope: SessionCommandScope, values: TaskWait, invocation: Invocation
) -> dict[str, JsonValue]:
    deadline = min(scope.deadline, time.monotonic() + values.seconds)
    operation = await _operation(service, scope.actor, values.operation_id, scope.session_id)
    if operation["project_id"] != values.project_id:
        raise ServiceError("permission_denied")
    while True:
        state, receipt, _record = await _observe(
            service, scope.actor, operation, scope.session_id, invocation.run_id, scope.check
        )
        result = _view(operation, state, receipt)
        if state in {"finished", "unknown"} or time.monotonic() >= deadline:
            return result
        scope.check()
        await asyncio.sleep(min(0.2, max(0.0, deadline - time.monotonic())))


async def cancel(
    service: SandboxService, scope: SessionCommandScope, values: TaskCancel, invocation: Invocation
) -> dict[str, JsonValue]:
    operation = await _operation(service, scope.actor, values.operation_id, scope.session_id)
    if operation["project_id"] != values.project_id:
        raise ServiceError("permission_denied")
    project = await service.project(scope.actor, operation["project_id"], write=True)
    if project["generation"] != values.expected_generation:
        raise ServiceError("input_revision_conflict")
    deadline = time.monotonic() + 18
    while True:
        state, receipt, _record = await _observe(
            service,
            scope.actor,
            operation,
            scope.session_id,
            invocation.run_id,
            scope.check,
            cancel=True,
        )
        if state in {"finished", "unknown"}:
            await service.store.append(
                invocation.run_id,
                "sandbox_task_cancelled",
                {"operation_id": values.operation_id, "status": state},
                event_id="sandbox-task-cancelled:" + values.operation_id,
            )
            return _view(operation, state, receipt)
        if time.monotonic() >= deadline:
            raise ServiceError("sandbox_cleanup_required")
        await asyncio.sleep(0.2)


async def _run_operations(service: SandboxService, run_id: str) -> list[dict[str, Any]]:
    async with service.store.lock, service.store.engine.connect() as conn:
        rows = (
            await conn.execute(
                select(sandbox_operations).where(
                    sandbox_operations.c.kind == "command",
                    sandbox_operations.c.result["run_id"].as_string() == run_id,
                )
            )
        ).mappings().all()
    return [dict(row) for row in rows if row["result"].get("background") is True]


async def cancel_run_tasks(service: SandboxService, run_id: str) -> None:
    """Stop any run-owned worker before a non-parked run is finalized."""
    rows = await _run_operations(service, run_id)
    for operation in rows:
        if operation["status"] in {"completed", "unknown"}:
            continue
        actor = service.identities.resolve(operation["principal_id"])
        session_id = operation["result"].get("session_id")
        if not isinstance(session_id, str):
            raise ServiceError("facts_corrupt")
        deadline = time.monotonic() + 18
        while True:
            state, _receipt, _record = await _observe(
                service,
                actor,
                operation,
                session_id,
                run_id,
                lambda: None,
                cancel=True,
            )
            if state in {"finished", "unknown"}:
                break
            if time.monotonic() >= deadline:
                raise ServiceError("sandbox_cleanup_required")
            await asyncio.sleep(0.2)


async def verify_completion(
    service: SandboxService,
    run_id: str,
    session_id: str,
    contract: SandboxCompletion,
    *,
    deadline: float,
    check: Callable[[], None],
) -> dict[str, JsonValue]:
    """Evaluate receipts and bounded file checks; polling never renews job permits."""
    rows = await _run_operations(service, run_id)
    deadline_mono = min(deadline, time.monotonic() + 150)
    while rows:
        check()
        pending = False
        actor = service.identities.resolve(rows[0]["principal_id"])
        for operation in rows:
            if operation["status"] not in {"completed", "unknown"}:
                task_state, _receipt_value, _ = await _observe(
                    service, actor, operation, session_id, run_id, check
                )
                pending |= task_state not in {"finished", "unknown"}
        if not pending:
            break
        if time.monotonic() >= deadline_mono:
            break
        await asyncio.sleep(0.2)
        rows = await _run_operations(service, run_id)
    rows = await _run_operations(service, run_id)
    receipts: list[dict[str, Any]] = []
    for operation in rows:
        receipt = operation["result"].get("receipt")
        if isinstance(receipt, dict):
            receipts.append({"operation_id": operation["operation_id"], **receipt})
    task_results = []
    verification_state: Literal["passed", "failed", "unknown"] = "passed"
    if len(receipts) < contract.minimum_tasks:
        verification_state = "failed"
    for receipt in receipts:
        valid = (
            receipt.get("executor_started") is True
            and receipt.get("output_complete") is True
            and receipt.get("cleanup") == "clean"
            and receipt.get("code") is None
            and receipt.get("verification") == "not_evaluated"
        )
        if contract.require_exit_zero:
            valid = valid and type(receipt.get("exit_code")) is int and receipt["exit_code"] == 0
        result_state = "passed" if valid else (
            "unknown"
            if receipt.get("cleanup") != "clean" or receipt.get("output_complete") is not True
            else "failed"
        )
        if result_state != "passed":
            verification_state = "unknown" if result_state == "unknown" else "failed"
        task_results.append({"operation_id": receipt["operation_id"], "state": result_state})
    condition_results: list[dict[str, JsonValue]] = []
    if (
        receipts
        and verification_state == "passed"
        and (contract.required_paths or contract.conditions)
    ):
        actor = service.identities.resolve(rows[0]["principal_id"])
        project_id = rows[0]["project_id"]
        contents: dict[str, bytes] = {}
        for path in {*contract.required_paths, *(item.path for item in contract.conditions)}:
            check()
            try:
                read_result = await service.read_file(
                    actor, project_id, path=path, offset=0, limit=65536
                )
                data = base64.b64decode(read_result["data"], validate=True)
                if len(data) != read_result["size"] or len(data) > 65536:
                    raise ValueError("bounded read mismatch")
                contents[path] = data
            except (ServiceError, ValueError, KeyError, TypeError):
                verification_state = "failed"
                contents[path] = b""
                condition_results.append({"path": path, "state": "failed"})
        for path in contract.required_paths:
            if path in contents:
                condition_results.append({"path": path, "state": "passed"})
        from krim_agent.workspace.verification import _condition

        for condition in contract.conditions:
            check()
            try:
                passed = _condition(
                    condition.kind,
                    contents[condition.path],
                    pointer=condition.pointer,
                    expected=condition.expected,
                    symbols=condition.symbols,
                )
                condition_results.append(
                    {"id": condition.id, "state": "passed" if passed else "failed"}
                )
                if not passed:
                    verification_state = "failed"
            except (ValueError, UnicodeError, KeyError, TypeError):
                verification_state = "failed"
                condition_results.append({"id": condition.id, "state": "failed"})
    result: dict[str, JsonValue] = {
        "schema": "sandbox-completion/1",
        "state": verification_state,
        "contract_fingerprint": fingerprint(contract.model_dump(mode="json")),
        "tasks": cast(JsonValue, task_results),
        "conditions": cast(JsonValue, condition_results),
        "evaluated_at": time.time(),
    }
    await _record_verification(service, rows, verification_state, result)
    await service.store.append(
        run_id,
        "sandbox_completion_verified",
        result,
        event_id="sandbox-completion-verification",
    )
    return result


async def _record_verification(
    service: SandboxService,
    rows: list[dict[str, Any]],
    state: str,
    verification: dict[str, JsonValue],
) -> None:
    async def write(conn: AsyncConnection) -> None:
        now = time.time()
        for operation in rows:
            current = (
                await conn.execute(
                    select(sandbox_operations).where(
                        sandbox_operations.c.operation_id == operation["operation_id"]
                    )
                )
            ).mappings().one()
            result = dict(current["result"])
            receipt = result.get("receipt")
            if not isinstance(receipt, dict):
                continue
            if receipt.get("verification") != "not_evaluated":
                if receipt.get("completion_verification") != verification:
                    raise ServiceError("target_binding_changed")
                continue
            receipt["verification"] = state
            receipt["completion_verification"] = verification
            result["receipt"] = receipt
            await conn.execute(
                update(sandbox_operations)
                .where(sandbox_operations.c.operation_id == operation["operation_id"])
                .values(result=result, updated_at=now)
            )

    await service.store._write(write)
