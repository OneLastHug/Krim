"""Register existing persistent services and revalidate every preview use."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace

from krim_agent.contracts import ServiceError
from krim_agent.development.contracts import (
    PreviewHandle,
    ProcessBackend,
    ProcessHandle,
)
from krim_agent.development.ports import EnvironmentRepository, ProcessRepository, command_digest
from krim_agent.development.preview_ports import PreviewRoutes, RouteBinding, RouteLease
from krim_agent.security import PrincipalContext

from .repository import (
    PreviewAuthorizer,
    PreviewRecord,
    PreviewRepository,
)


class PreviewError(Exception):
    """Safe fixed error code; never includes upstream addresses or exceptions."""

    def __init__(self, code: str, status: int = 409) -> None:
        self.code, self.status = code, status
        super().__init__(code)


def identity(process: ProcessHandle) -> tuple[str, str, str, int]:
    env = process.environment
    return env.owner_id, env.project_id, env.environment_id, env.generation


def binding(record: PreviewRecord) -> RouteBinding:
    env = record.handle.environment
    return RouteBinding(
        preview_id=record.handle.preview_id,
        owner_id=env.owner_id,
        project_id=env.project_id,
        environment_id=env.environment_id,
        generation=env.generation,
        service_id=record.handle.service_id,
        process_id=record.process.process_id,
        operation_id=record.process.operation_id,
        request_digest=record.request_digest,
        execution_id=record.instance.execution_id,
        started_at=record.instance.started_at,
        port=record.handle.port,
        route_epoch=record.route_epoch,
    )


def validate_record(record: PreviewRecord) -> None:
    env = record.handle.environment
    if (
        (env.owner_id, env.project_id, env.environment_id, env.generation)
        != identity(record.process)
        or record.handle.service_id != record.process.process_id
        or record.process.lifecycle != "project_service"
        or record.authorization.project_id != env.project_id
        or not record.instance.execution_id
        or record.revision < 1
    ):
        raise PreviewError("preview_binding_drift")
    try:
        binding(record)
    except ValueError:
        raise PreviewError("preview_binding_drift") from None


class PreviewService:
    def __init__(
        self,
        repository: PreviewRepository,
        processes: ProcessRepository,
        environments: EnvironmentRepository,
        backend: ProcessBackend,
        authorizer: PreviewAuthorizer,
        routes: PreviewRoutes,
    ) -> None:
        self.repository = repository
        self.processes = processes
        self.environments = environments
        self.backend = backend
        self.authorizer = authorizer
        self.routes = routes

    async def register(
        self,
        preview_id: str,
        process: ProcessHandle,
        port: int,
        principal: PrincipalContext,
    ) -> PreviewHandle:
        row = await self.processes.get(process.process_id)
        try:
            row.handle.require_binding(process)
            row.handle.require_binding(row.request)
        except ValueError:
            raise PreviewError("preview_binding_drift") from None
        if (
            row.execution_id is None
            or row.request_digest != command_digest(row.request)
            or row.handle.lifecycle != "project_service"
        ):
            raise PreviewError("preview_binding_drift")
        handle = PreviewHandle(
            preview_id=preview_id,
            environment=row.handle.environment,
            service_id=row.handle.process_id,
            port=port,
            state="pending",
            access_path=f"/api/development/previews/{preview_id}/open",
        )
        # Shared observe requires B to call I's source.registration first.
        # E does not call that source recursively through its runtime checks.
        instance = await self.routes.observe(row.handle, port, principal)
        if instance.execution_id != row.execution_id:
            raise PreviewError("preview_binding_drift")
        record = PreviewRecord(
            handle=handle,
            process=row.handle,
            authorization=row.request.authorization,
            instance=instance,
            request_digest=row.request_digest,
        )
        validate_record(record)
        await self.authorizer.check(record, principal, "register")
        await self._live(record)
        stored = await self.repository.create(record)
        if stored != record:
            raise PreviewError("preview_binding_drift")
        return stored.handle

    async def _live(self, record: PreviewRecord) -> None:
        validate_record(record)
        row = await self.processes.get(record.process.process_id)
        try:
            row.handle.require_binding(record.process)
            row.handle.require_binding(row.request)
        except ValueError:
            raise PreviewError("preview_binding_drift") from None
        if (
            row.execution_id != record.instance.execution_id
            or row.request.authorization != record.authorization
            or row.request_digest != record.request_digest
            or row.request_digest != command_digest(row.request)
        ):
            raise PreviewError("preview_binding_drift")
        env = await self.environments.resolve(record.handle.environment)
        expected = record.handle.environment
        actual = env.environment
        if (actual.owner_id, actual.project_id, actual.environment_id, actual.generation) != (
            expected.owner_id,
            expected.project_id,
            expected.environment_id,
            expected.generation,
        ):
            raise PreviewError("preview_binding_drift")
        if actual.state != "ready":
            raise PreviewError("preview_inactive", 410)
        observed = await self.backend.inspect(row.handle)
        try:
            observed.require_binding(row.handle)
        except ValueError:
            raise PreviewError("preview_binding_drift") from None
        if row.handle.state != "running" or observed.state != "running":
            raise PreviewError("preview_inactive", 410)

    async def check(
        self,
        preview_id: str,
        principal: PrincipalContext | None,
        *,
        action: str = "serve",
        expected: PreviewRecord | None = None,
    ) -> PreviewRecord:
        record = await self.repository.get(preview_id)
        validate_record(record)
        if record.handle.preview_id != preview_id:
            raise PreviewError("preview_binding_drift")
        if expected is not None and (
            binding(record) != binding(expected)
            or record.authorization != expected.authorization
            or record.process.ownership != expected.process.ownership
            or record.process.operation_id != expected.process.operation_id
            or record.process.log_ref != expected.process.log_ref
        ):
            raise PreviewError("preview_binding_drift")
        if record.handle.state not in {"ready", "pending"}:
            raise PreviewError("preview_inactive", 410)
        if action == "open":
            await self.authorizer.check(record, principal, "open")
        else:
            await self.authorizer.check(record, principal, "serve")
        await self._live(record)
        return record

    @asynccontextmanager
    async def route(
        self,
        preview_id: str,
        principal: PrincipalContext | None,
    ) -> AsyncIterator[tuple[PreviewRecord, RouteLease]]:
        record = await self.check(preview_id, principal)
        async with self.routes.acquire(binding(record), principal) as lease:
            if lease.binding != binding(record):
                raise PreviewError("preview_binding_drift")
            await self.recheck(record, principal, lease)
            yield record, lease

    async def recheck(
        self,
        record: PreviewRecord,
        principal: PrincipalContext | None,
        lease: RouteLease,
    ) -> None:
        if binding(record) != lease.binding:
            raise PreviewError("preview_binding_drift")
        await self.check(record.handle.preview_id, principal, expected=record)
        await self.routes.check(lease)

    async def wait_ready(self, preview_id: str, *, wait_seconds: float = 5.0) -> PreviewHandle:
        if not 0 < wait_seconds <= 30:
            raise ValueError("readiness timeout must be in (0, 30]")
        try:
            async with asyncio.timeout(wait_seconds):
                async with self.route(preview_id, None) as (record, lease):
                    while not await self.routes.ready(lease):
                        await asyncio.sleep(0.05)
                        await self.recheck(record, None, lease)
                    await self.recheck(record, None, lease)
                    latest = await self.check(preview_id, None, expected=record)
                    if latest.handle.state == "ready":
                        return latest.handle
                    updated = replace(
                        latest,
                        handle=latest.handle.model_copy(update={"state": "ready"}),
                        revision=latest.revision + 1,
                    )
                    return (await self.repository.save(latest, updated)).handle
        except TimeoutError:
            raise PreviewError("preview_not_ready", 503) from None

    async def stop(self, preview_id: str, principal: PrincipalContext) -> PreviewHandle:
        record = await self.repository.get(preview_id)
        validate_record(record)
        await self.authorizer.check(record, principal, "stop")
        if record.handle.state not in {"stopped", "revoked"}:
            record = await self.repository.save(
                record,
                replace(
                    record,
                    handle=record.handle.model_copy(update={"state": "stopped"}),
                    route_epoch=record.route_epoch + 1,
                    revision=record.revision + 1,
                ),
            )
        # Fence first. A cancellation failure cannot reopen proxy traffic. Retry
        # stop even for already-stopped rows to finish a previous failed cleanup.
        result = await self.backend.cancel(record.process)
        try:
            result.require_binding(record.process)
        except ValueError:
            raise PreviewError("preview_binding_drift") from None
        if result.state not in {"cancelled", "exited", "failed"}:
            raise PreviewError("preview_stop_incomplete")
        return record.handle

    async def revoke(self, preview_id: str) -> PreviewHandle:
        """Trusted host cleanup hook; never expose as an unauthenticated route."""
        for _ in range(3):
            record = await self.repository.get(preview_id)
            validate_record(record)
            if record.handle.state == "revoked":
                return record.handle
            updated = replace(
                record,
                handle=record.handle.model_copy(update={"state": "revoked"}),
                route_epoch=record.route_epoch + 1,
                revision=record.revision + 1,
            )
            try:
                return (await self.repository.save(record, updated)).handle
            except ServiceError as exc:
                if exc.code != "revision_conflict":
                    raise
        raise PreviewError("preview_revision_conflict")
