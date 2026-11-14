"""I-owned SQLite adapters for development environment and process ports."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from krim_agent.contracts import ServiceError
from krim_agent.development.contracts import (
    CommandRequest,
    EnvironmentHandle,
    OperationAdmission,
    OperationIdentity,
    OperationReceipt,
    ProcessHandle,
    RunOwnership,
)
from krim_agent.development.ports import (
    EnvironmentRecord,
    ProcessRecord,
    ProcessReservation,
    command_digest,
)

from .development_models import development_environments as environments
from .development_models import development_operations as operations
from .development_models import development_processes as processes
from .models import runs, sessions, turns
from .sandbox_models import sandbox_projects

if TYPE_CHECKING:
    from .sqlite import SQLiteStore


def environment_binding(value: EnvironmentHandle) -> tuple[str, str, str, int]:
    return value.owner_id, value.project_id, value.environment_id, value.generation


def _environment_json(record: EnvironmentRecord) -> dict[str, Any]:
    value = asdict(record)
    value["environment"] = record.environment.model_dump(mode="json")
    value["content_path"] = str(record.content_path)
    return value


def _environment_record(value: dict[str, Any]) -> EnvironmentRecord:
    return EnvironmentRecord(**{
        **value, "environment": EnvironmentHandle.model_validate(value["environment"]),
        "content_path": Path(value["content_path"]),
    })


def _process_record(row: Any) -> ProcessRecord:
    record = ProcessRecord(
        request=CommandRequest.model_validate_json(json.dumps(row["request"])),
        handle=ProcessHandle.model_validate(row["handle"]),
        request_digest=row["request_digest"], revision=row["revision"],
        execution_id=row["execution_id"],
    )
    record.handle.require_binding(record.request)
    expected = {
        "process_id": record.handle.process_id, "operation_id": record.request.operation_id,
        "environment_id": record.request.environment.environment_id,
        "owner_id": record.request.ownership.owner_id,
        "project_id": record.request.environment.project_id,
        "session_id": record.request.ownership.session_id,
        "run_id": record.request.ownership.run_id,
        "lifecycle": record.request.lifecycle, "state": record.handle.state,
    }
    if (record.request_digest != command_digest(record.request)
            or any(row[key] != value for key, value in expected.items())):
        raise ServiceError("facts_corrupt")
    return record


class DevelopmentStore:
    """All three repositories share the existing store lock and transactions."""

    def __init__(self, store: SQLiteStore) -> None:
        self.store = store
        self.environments = EnvironmentRecords(self)
        self.operations = OperationRecords(self)
        self.processes = ProcessRecords(self)

    def require(self) -> None:
        if self.store.schema_version < 15:
            raise ServiceError("schema_upgrade_required")

    async def current_environment(
        self, conn: AsyncConnection, environment: EnvironmentHandle,
    ) -> EnvironmentRecord:
        row = (await conn.execute(select(environments).where(
            environments.c.environment_id == environment.environment_id,
        ))).mappings().first()
        if row is None:
            raise ServiceError("not_found", 404)
        record = _environment_record(row["record"])
        expected = {
            "environment_id": record.environment.environment_id,
            "owner_id": record.environment.owner_id, "project_id": record.environment.project_id,
            "generation": record.environment.generation, "revision": record.revision,
        }
        if any(row[key] != value for key, value in expected.items()):
            raise ServiceError("facts_corrupt")
        if environment_binding(record.environment) != environment_binding(environment):
            raise ServiceError("tool_binding_changed")
        return record

    async def ownership(
        self, conn: AsyncConnection, environment: EnvironmentHandle, ownership: RunOwnership,
    ) -> None:
        record = await self.current_environment(conn, environment)
        if record.environment.state != "ready":
            raise ServiceError("workspace_busy")
        project = (await conn.execute(select(sandbox_projects).where(
            sandbox_projects.c.project_id == environment.project_id,
        ))).mappings().one()
        session = (await conn.execute(select(sessions).where(
            sessions.c.id == ownership.session_id,
        ))).mappings().first()
        run = (await conn.execute(select(runs).where(
            runs.c.id == ownership.run_id,
        ))).mappings().first()
        turn = (await conn.execute(select(turns).where(
            turns.c.run_id == ownership.run_id,
        ))).mappings().first()
        if (turn is None or turn["session_id"] != ownership.session_id
                or turn["owner_id"] != ownership.owner_id):
            raise ServiceError("permission_denied")
        if (ownership.owner_id != environment.owner_id
                or project["principal_id"] != ownership.owner_id
                or session is None or session["owner_id"] != ownership.owner_id
                or session["workspace_id"] != project["workspace_id"] or run is None):
            raise ServiceError("permission_denied")
        if run["stop_requested"] or run["status"] != "running":
            raise ServiceError("cancelled" if run["stop_requested"] else "run_closed")


class EnvironmentRecords:
    def __init__(self, owner: DevelopmentStore) -> None:
        self.owner = owner

    async def register(self, record: EnvironmentRecord) -> EnvironmentRecord:
        """Trusted provisioning entry; it does not create directories or containers."""
        self.owner.require()
        if (record.revision != 1 or record.container_id is not None
                or type(record.max_processes) is not int or not 1 <= record.max_processes <= 32):
            raise ServiceError("invalid_input", 422)

        async def write(conn: AsyncConnection) -> EnvironmentRecord:
            project = (await conn.execute(select(sandbox_projects).where(
                sandbox_projects.c.project_id == record.environment.project_id,
            ))).mappings().first()
            if project is None or project["principal_id"] != record.environment.owner_id:
                raise ServiceError("permission_denied")
            prior = (await conn.execute(select(environments).where(
                environments.c.project_id == record.environment.project_id,
            ))).mappings().first()
            if prior is not None:
                found = _environment_record(prior["record"])
                if found != record:
                    raise ServiceError("key_conflict")
                return found
            now = time.time()
            await conn.execute(insert(environments).values(
                environment_id=record.environment.environment_id,
                project_id=record.environment.project_id, owner_id=record.environment.owner_id,
                generation=record.environment.generation, revision=record.revision,
                record=_environment_json(record), created_at=now, updated_at=now,
            ))
            return record

        async def confirm() -> EnvironmentRecord | None:
            try:
                found = await self.resolve(record.environment)
            except ServiceError:
                return None
            return found if found == record else None

        return await self.owner.store._eval_write(write, confirm)

    async def resolve(self, environment: EnvironmentHandle) -> EnvironmentRecord:
        self.owner.require()
        async with self.owner.store.lock, self.owner.store.engine.connect() as conn:
            return await self.owner.current_environment(conn, environment)

    async def begin_recreate(self, expected: EnvironmentRecord) -> EnvironmentRecord:
        """Persist the previous binding before B removes or recreates any container."""
        self.owner.require()
        if expected.environment.state not in {"stopped", "stopping"}:
            raise ServiceError("workspace_busy")
        if expected.container_id is None:
            raise ServiceError("invalid_input", 422)
        updated = replace(
            expected, revision=expected.revision + 1, container_id=None,
            configuration_digest=None,
            environment=expected.environment.model_copy(update={
                "generation": expected.environment.generation + 1, "state": "creating",
            }),
        )

        async def write(conn: AsyncConnection) -> EnvironmentRecord:
            current = await self.owner.current_environment(conn, expected.environment)
            if current != expected:
                raise ServiceError("revision_conflict")
            prior = (await conn.execute(select(environments.c.previous_record).where(
                environments.c.environment_id == expected.environment.environment_id,
            ))).scalar_one_or_none()
            if prior is not None:
                raise ServiceError("sandbox_cleanup_required")
            await conn.execute(update(environments).where(
                environments.c.environment_id == expected.environment.environment_id,
            ).values(generation=updated.environment.generation, revision=updated.revision,
                     record=_environment_json(updated), previous_record=_environment_json(expected),
                     updated_at=time.time()))
            return updated

        async def confirm() -> EnvironmentRecord | None:
            found = await self.resolve(updated.environment)
            return found if found == updated else None

        return await self.owner.store._eval_write(write, confirm)

    async def previous(self, current: EnvironmentHandle) -> EnvironmentRecord:
        """A host-only cleanup receipt, never a user authorization."""
        self.owner.require()
        async with self.owner.store.lock, self.owner.store.engine.connect() as conn:
            await self.owner.current_environment(conn, current)
            raw = (await conn.execute(select(environments.c.previous_record).where(
                environments.c.environment_id == current.environment_id,
            ))).scalar_one_or_none()
            if raw is None:
                raise ServiceError("not_found", 404)
            old = _environment_record(raw)
            if (environment_binding(old.environment)[:3] != environment_binding(current)[:3]
                    or old.environment.generation + 1 != current.generation):
                raise ServiceError("facts_corrupt")
            return old

    async def finish_recreate(self, current: EnvironmentHandle) -> None:
        """Forget a retired binding only after all its process rows are terminal."""
        self.owner.require()
        old = await self.previous(current)

        async def write(conn: AsyncConnection) -> bool:
            registered = await self.owner.current_environment(conn, current)
            if registered.environment.state != "ready":
                raise ServiceError("workspace_busy")
            raw = (await conn.execute(select(environments.c.previous_record).where(
                environments.c.environment_id == current.environment_id,
            ))).scalar_one_or_none()
            if raw != _environment_json(old):
                raise ServiceError("revision_conflict")
            rows = (await conn.execute(select(processes).where(
                processes.c.environment_id == current.environment_id,
            ))).mappings().all()
            for row in rows:
                process = _process_record(row)
                if (process.request.environment.generation == old.environment.generation
                        and process.handle.state not in {"exited", "cancelled", "failed"}):
                    raise ServiceError("sandbox_cleanup_required")
            await conn.execute(update(environments).where(
                environments.c.environment_id == current.environment_id,
            ).values(previous_record=None, updated_at=time.time()))
            return True

        async def confirm() -> bool | None:
            async with self.owner.store.lock, self.owner.store.engine.connect() as conn:
                await self.owner.current_environment(conn, current)
                row = (await conn.execute(select(environments.c.previous_record).where(
                    environments.c.environment_id == current.environment_id,
                ))).one()
                return True if row[0] is None else None

        await self.owner.store._eval_write(write, confirm)

    async def compare_and_swap(
        self, expected: EnvironmentRecord, updated: EnvironmentRecord,
    ) -> EnvironmentRecord:
        self.owner.require()
        if (environment_binding(expected.environment) != environment_binding(updated.environment)
                or updated.revision != expected.revision + 1
                or replace(expected, environment=updated.environment,
                           revision=updated.revision, container_id=updated.container_id,
                           configuration_digest=updated.configuration_digest) != updated):
            raise ServiceError("input_revision_conflict")

        async def write(conn: AsyncConnection) -> EnvironmentRecord:
            current = await self.owner.current_environment(conn, expected.environment)
            if current != expected:
                raise ServiceError("revision_conflict")
            await conn.execute(update(environments).where(
                environments.c.environment_id == expected.environment.environment_id,
                environments.c.revision == expected.revision,
            ).values(revision=updated.revision, record=_environment_json(updated),
                     updated_at=time.time()))
            return updated

        async def confirm() -> EnvironmentRecord | None:
            current = await self.resolve(updated.environment)
            return current if current == updated else None

        return await self.owner.store._eval_write(write, confirm)


class OperationRecords:
    def __init__(self, owner: DevelopmentStore) -> None:
        self.owner = owner

    async def _lookup(
        self, conn: AsyncConnection, identity: OperationIdentity,
    ) -> OperationReceipt | None:
        row = (await conn.execute(select(operations.c.receipt).where(
            operations.c.operation_id == identity.operation_id,
        ))).scalar_one_or_none()
        if row is None:
            return None
        receipt = OperationReceipt.model_validate(row)
        if receipt.identity != identity:
            raise ServiceError("key_conflict")
        return receipt

    async def lookup(self, identity: OperationIdentity) -> OperationReceipt | None:
        self.owner.require()
        async with self.owner.store.lock, self.owner.store.engine.connect() as conn:
            await self.owner.current_environment(conn, identity.environment)
            return await self._lookup(conn, identity)

    async def _admit(
        self, conn: AsyncConnection, identity: OperationIdentity,
    ) -> OperationAdmission:
        existing = await self._lookup(conn, identity)
        if existing is not None:
            return OperationAdmission(receipt=existing, execute=False)
        await self.owner.ownership(conn, identity.environment, identity.ownership)
        now = time.time()
        receipt = OperationReceipt(identity=identity, state="accepted")
        await conn.execute(insert(operations).values(
            operation_id=identity.operation_id, environment_id=identity.environment.environment_id,
            owner_id=identity.ownership.owner_id, project_id=identity.environment.project_id,
            session_id=identity.ownership.session_id, run_id=identity.ownership.run_id,
            identity=identity.model_dump(mode="json"), receipt=receipt.model_dump(mode="json"),
            created_at=now, updated_at=now,
        ))
        return OperationAdmission(receipt=receipt, execute=True)

    async def admit(self, identity: OperationIdentity) -> OperationAdmission:
        self.owner.require()

        async def write(conn: AsyncConnection) -> OperationAdmission:
            return await self._admit(conn, identity)

        async def confirm() -> OperationAdmission | None:
            found = await self.lookup(identity)
            # A lost COMMIT response is not a fresh execution permit.
            return OperationAdmission(receipt=found, execute=False) if found else None

        return await self.owner.store._eval_write(write, confirm)

    async def bind_command(self, identity: OperationIdentity, request: CommandRequest) -> None:
        """Trusted SDK tool bridge binds its exact command without reusing auth hashes."""
        self.owner.require()
        if (identity.operation_id != request.operation_id
                or environment_binding(identity.environment)
                != environment_binding(request.environment)
                or identity.ownership != request.ownership):
            raise ServiceError("key_conflict")
        digest = command_digest(request)

        async def write(conn: AsyncConnection) -> bool:
            receipt = await self._lookup(conn, identity)
            if receipt is None or receipt.state != "accepted":
                raise ServiceError("sandbox_operation_unknown")
            row = (await conn.execute(select(operations.c.command_digest).where(
                operations.c.operation_id == identity.operation_id,
            ))).one()
            if row[0] is not None and row[0] != digest:
                raise ServiceError("key_conflict")
            await conn.execute(update(operations).where(
                operations.c.operation_id == identity.operation_id,
            ).values(command_digest=digest, updated_at=time.time()))
            return True

        async def confirm() -> bool | None:
            async with self.owner.store.lock, self.owner.store.engine.connect() as conn:
                await self._lookup(conn, identity)
                value = (await conn.execute(select(operations.c.command_digest).where(
                    operations.c.operation_id == identity.operation_id,
                ))).scalar_one_or_none()
                return True if value == digest else None

        await self.owner.store._eval_write(write, confirm)

    async def save_receipt(self, receipt: OperationReceipt) -> None:
        self.owner.require()

        async def write(conn: AsyncConnection) -> bool:
            previous = await self._lookup(conn, receipt.identity)
            if previous is None:
                raise ServiceError("not_found", 404)
            if previous == receipt:
                return True
            if previous.state in {"completed", "failed", "cancelled"}:
                raise ServiceError("key_conflict")
            if receipt.state == "accepted" and previous.state != "accepted":
                raise ServiceError("key_conflict")
            await conn.execute(update(operations).where(
                operations.c.operation_id == receipt.identity.operation_id,
            ).values(receipt=receipt.model_dump(mode="json"), updated_at=time.time()))
            return True

        async def confirm() -> bool | None:
            return True if await self.lookup(receipt.identity) == receipt else None

        await self.owner.store._eval_write(write, confirm)


class ProcessRecords:
    def __init__(self, owner: DevelopmentStore) -> None:
        self.owner = owner

    async def reserve(self, request: CommandRequest, request_digest: str) -> ProcessReservation:
        self.owner.require()
        if request_digest != command_digest(request):
            raise ServiceError("key_conflict")

        async def write(conn: AsyncConnection) -> ProcessReservation:
            row = (await conn.execute(select(processes).where(
                processes.c.operation_id == request.operation_id,
            ))).mappings().first()
            if row is not None:
                previous = _process_record(row)
                if previous.request_digest != request_digest:
                    raise ServiceError("key_conflict")
                return ProcessReservation(previous, False)
            await self.owner.ownership(conn, request.environment, request.ownership)
            environment = await self.owner.current_environment(conn, request.environment)
            active = await conn.scalar(select(func.count()).select_from(processes).where(
                processes.c.environment_id == request.environment.environment_id,
                processes.c.state.in_(["accepted", "running", "unknown"]),
            ))
            if (active or 0) >= environment.max_processes:
                raise ServiceError("capacity", 429)
            # SDK may have admitted this same operation just before invoking the
            # command tool. Join it, never dispatch or create a second ledger.
            op = (await conn.execute(select(operations).where(
                operations.c.operation_id == request.operation_id,
            ))).mappings().first()
            if op is None:
                identity = OperationIdentity(
                    operation_id=request.operation_id, call_id=request.operation_id,
                    environment=request.environment, ownership=request.ownership,
                    request_digest=request_digest,
                )
                await self.owner.operations._admit(conn, identity)
                await conn.execute(update(operations).where(
                    operations.c.operation_id == request.operation_id,
                ).values(command_digest=request_digest))
            else:
                identity = OperationIdentity.model_validate(op["identity"])
                receipt = OperationReceipt.model_validate(op["receipt"])
                if (environment_binding(identity.environment)
                        != environment_binding(request.environment)
                        or identity.ownership != request.ownership
                        or op["command_digest"] != request_digest):
                    raise ServiceError("key_conflict")
                if receipt.state != "accepted":
                    raise ServiceError("sandbox_operation_unknown")
            handle = ProcessHandle(
                process_id=str(uuid.uuid4()), operation_id=request.operation_id,
                environment=request.environment, ownership=request.ownership,
                lifecycle=request.lifecycle, state="accepted", log_ref="log-" + uuid.uuid4().hex,
            )
            now = time.time()
            await conn.execute(insert(processes).values(
                process_id=handle.process_id, operation_id=request.operation_id,
                environment_id=request.environment.environment_id,
                owner_id=request.ownership.owner_id, project_id=request.environment.project_id,
                session_id=request.ownership.session_id, run_id=request.ownership.run_id,
                lifecycle=request.lifecycle, state=handle.state, request_digest=request_digest,
                request=request.model_dump(mode="json"), handle=handle.model_dump(mode="json"),
                revision=1, execution_id=None, created_at=now, updated_at=now,
            ))
            return ProcessReservation(ProcessRecord(request, handle, request_digest), True)

        async def confirm() -> ProcessReservation | None:
            async with self.owner.store.lock, self.owner.store.engine.connect() as conn:
                row = (await conn.execute(select(processes).where(
                    processes.c.operation_id == request.operation_id,
                ))).mappings().first()
                if row is None:
                    return None
                record = _process_record(row)
                if record.request_digest != request_digest:
                    raise ServiceError("key_conflict")
                return ProcessReservation(record, False)

        return await self.owner.store._eval_write(write, confirm)

    async def get(self, process_id: str) -> ProcessRecord:
        self.owner.require()
        async with self.owner.store.lock, self.owner.store.engine.connect() as conn:
            row = (await conn.execute(select(processes).where(
                processes.c.process_id == process_id,
            ))).mappings().first()
            if row is None:
                raise ServiceError("not_found", 404)
            return _process_record(row)

    async def save(self, expected: ProcessRecord, updated: ProcessRecord) -> ProcessRecord:
        self.owner.require()
        updated.handle.require_binding(expected.handle)
        if (updated.request != expected.request or updated.request_digest != expected.request_digest
                or updated.revision != expected.revision + 1
                or (expected.execution_id is not None
                    and expected.execution_id != updated.execution_id)):
            raise ServiceError("input_revision_conflict")
        allowed = {
            "accepted": {"accepted", "running", "exited", "failed", "cancelled", "unknown"},
            "running": {"running", "exited", "failed", "cancelled", "unknown"},
            "unknown": {"unknown", "exited", "failed", "cancelled"},
            "exited": {"exited"}, "failed": {"failed"}, "cancelled": {"cancelled"},
        }
        if updated.handle.state not in allowed[expected.handle.state]:
            raise ServiceError("key_conflict")
        if expected.handle.state in {"exited", "failed", "cancelled"} and (
            updated.handle != expected.handle
        ):
            raise ServiceError("key_conflict")

        async def write(conn: AsyncConnection) -> ProcessRecord:
            row = (await conn.execute(select(processes).where(
                processes.c.process_id == expected.handle.process_id,
            ))).mappings().first()
            if row is None or _process_record(row) != expected:
                raise ServiceError("revision_conflict")
            await conn.execute(update(processes).where(
                processes.c.process_id == expected.handle.process_id,
                processes.c.revision == expected.revision,
            ).values(state=updated.handle.state, handle=updated.handle.model_dump(mode="json"),
                     revision=updated.revision, execution_id=updated.execution_id,
                     updated_at=time.time()))
            return updated

        async def confirm() -> ProcessRecord | None:
            found = await self.get(updated.handle.process_id)
            return found if found == updated else None

        return await self.owner.store._eval_write(write, confirm)

    async def list_environment(self, environment: EnvironmentHandle) -> tuple[ProcessRecord, ...]:
        self.owner.require()
        async with self.owner.store.lock, self.owner.store.engine.connect() as conn:
            await self.owner.current_environment(conn, environment)
            rows = (await conn.execute(select(processes).where(
                processes.c.environment_id == environment.environment_id,
            ))).mappings().all()
            records = tuple(_process_record(row) for row in rows)
            return tuple(record for record in records
                         if record.request.environment.generation == environment.generation)

    async def list_environment_for_cleanup(
        self, previous: EnvironmentHandle,
    ) -> tuple[ProcessRecord, ...]:
        self.owner.require()
        async with self.owner.store.lock, self.owner.store.engine.connect() as conn:
            raw = (await conn.execute(select(environments.c.previous_record).where(
                environments.c.environment_id == previous.environment_id,
            ))).scalar_one_or_none()
            if raw is None:
                raise ServiceError("not_found", 404)
            persisted = _environment_record(raw)
            if environment_binding(persisted.environment) != environment_binding(previous):
                raise ServiceError("tool_binding_changed")
            rows = (await conn.execute(select(processes).where(
                processes.c.environment_id == previous.environment_id,
            ))).mappings().all()
            records = tuple(_process_record(row) for row in rows)
            return tuple(record for record in records
                         if record.request.environment.generation == previous.generation)

    async def list_run(self, ownership: RunOwnership) -> tuple[ProcessRecord, ...]:
        self.owner.require()
        async with self.owner.store.lock, self.owner.store.engine.connect() as conn:
            rows = (await conn.execute(select(processes).where(
                processes.c.owner_id == ownership.owner_id,
                processes.c.session_id == ownership.session_id,
                processes.c.run_id == ownership.run_id,
            ))).mappings().all()
            return tuple(_process_record(row) for row in rows)
