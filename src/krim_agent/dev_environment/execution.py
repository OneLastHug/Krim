"""One native container per command/terminal/service, without extra capabilities."""
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Any

from krim_agent.development.contracts import CommandRequest
from krim_agent.development.ports import (
    ProcessRecord,
    ProcessRepository,
    RuntimeExecution,
    RuntimeStatus,
    command_digest,
)

from . import docker
from .backend import DevelopmentBackend
from .model import EnvironmentError, RuntimeSpec, identity


class ContainerExecutions:
    """B execution port. C owns admission, host supervisor, logs and launch receipt.

    No project command executes during prepare. The only launch descriptor is a
    fixed Docker start/attach to an already-created immutable container. Repeated
    prepare is refused; recovery uses inspect and never reruns start/attach.
    """

    def __init__(self, environments: DevelopmentBackend, processes: ProcessRepository) -> None:
        self.environments = environments
        self.processes = processes
        if environments.processes is not None and environments.processes is not processes:
            raise ValueError("environment and execution repositories must agree")
        environments.processes = processes

    @property
    def runner(self) -> docker.Runner:
        return self.environments.runner

    def _descriptor(self, spec: RuntimeSpec, execution_id: str) -> RuntimeExecution:
        assert spec.request is not None and spec.process_id is not None
        # env -i prevents DOCKER_CONFIG/context/plugin/proxy injection from a
        # service's ambient environment; a fixed local Unix socket is used.
        argv = ("/usr/bin/env", "-i", "PATH=/usr/bin:/bin", "HOME=/nonexistent", "LC_ALL=C",
                *self.runner.prefix, "start", "--attach", "--interactive", execution_id)
        return RuntimeExecution(execution_id, spec.handle, spec.request.operation_id,
                                spec.process_id, argv, spec.request.pty)

    @staticmethod
    def _validate_row(row: ProcessRecord, request: CommandRequest, process_id: str) -> None:
        try:
            row.handle.require_binding(request)
        except ValueError:
            raise EnvironmentError("execution_binding_drift") from None
        if (row.handle.process_id != process_id or row.request_digest != command_digest(request)
                or command_digest(row.request) != command_digest(request)):
            raise EnvironmentError("execution_binding_drift")

    @asynccontextmanager
    async def prepare(self, request: CommandRequest,
                      process_id: str) -> AsyncIterator[RuntimeExecution]:
        handle = request.environment
        async with self.environments.locks.hold(handle.owner_id, handle.project_id):
            env = await self.environments._resolve(handle, "execute")
            if env.handle.state != "ready":
                raise EnvironmentError("environment_not_ready")
            row = await self.processes.get(process_id)
            self._validate_row(row, request, process_id)
            if row.execution_id is not None or row.handle.state != "accepted":
                raise EnvironmentError("execution_already_prepared")
            rows = await self.processes.list_environment(handle)
            seen = set()
            admitted = False
            for reserved in rows:
                self._validate_row(reserved, reserved.request, reserved.handle.process_id)
                if (identity(reserved.request.environment) != identity(handle)
                        or reserved.handle.process_id in seen):
                    raise EnvironmentError("execution_binding_drift")
                seen.add(reserved.handle.process_id)
                if reserved.handle.process_id == process_id:
                    admitted = reserved == row
            if not admitted:
                raise EnvironmentError("execution_reservation_missing")
            state, cid = await self.environments._observation(env)
            if state != "ready" or cid is None:
                raise EnvironmentError("environment_not_ready")
            env = await self.environments._save(env, state, cid)
            spec = RuntimeSpec(env.record, request, process_id)
            existing = await docker.inspect(self.runner, spec)
            if existing is not None:
                docker.verify(spec, existing)
                # Receipt loss is not permission to repeat a launch. I/C must
                # reconcile the existing container and classify unknown.
                raise EnvironmentError("execution_outcome_unknown")
            await docker.check_image(self.runner, spec)
            await self.environments._network_check(spec)
            await self.environments.authorizer(env.handle, "execute")
            self.environments.mount_check(spec)
            await docker.checked(self.runner, docker.create_args(spec))
            value = await docker.inspect(self.runner, spec)
            if value is None:
                raise EnvironmentError("execution_create_unknown")
            docker.verify(spec, value)
            if value["State"]["Status"] != "created":
                raise EnvironmentError("execution_outcome_unknown")
            descriptor = self._descriptor(spec, str(value["Id"]))
            # Caller persists this ID before launching under C's supervisor.
            try:
                await self.environments._network_arm(spec, descriptor.execution_id)
                yield descriptor
                persisted = await self.processes.get(process_id)
                self._validate_row(persisted, request, process_id)
                if persisted.execution_id != descriptor.execution_id:
                    raise EnvironmentError("execution_receipt_missing")
            except BaseException:
                await self._abort_preparation(replace(spec, execution_id=descriptor.execution_id))
                raise

    async def _abort_preparation(self, spec: RuntimeSpec) -> None:
        """Compensate only this call's new unit while retaining the launch fence."""
        assert spec.process_id is not None and spec.request is not None
        assert spec.execution_id is not None
        row = await self.processes.get(spec.process_id)
        self._validate_row(row, spec.request, spec.process_id)
        if row.execution_id not in (None, spec.execution_id):
            raise EnvironmentError("execution_binding_drift")
        await self.environments._network_release(spec.execution_id)
        value = await docker.inspect(self.runner, spec)
        if value is None:
            raise EnvironmentError("execution_cleanup_unknown")
        await docker.stop_verified(self.runner, spec, value)
        stopped = await docker.inspect(self.runner, spec)
        if stopped is None or docker.verify(spec, stopped) != "stopped":
            raise EnvironmentError("execution_cleanup_unknown")
        code = stopped["State"].get("ExitCode")
        code = code if stopped["State"]["Status"] == "exited" and type(code) is int else None
        await self.environments._finish_process(row, "cancelled", code, spec.execution_id)
        await docker.checked(self.runner, ("rm", "--force", spec.execution_id))

    async def _bound(self, execution: RuntimeExecution, *, cleanup: bool = False) -> RuntimeSpec:
        row = await self.processes.get(execution.process_id)
        self._validate_row(row, row.request, execution.process_id)
        if ((row.execution_id != execution.execution_id
             and not (cleanup and row.execution_id is None))
                or row.request.operation_id != execution.operation_id
                or identity(row.request.environment) != identity(execution.environment)
                or row.request.pty != execution.pty):
            raise EnvironmentError("execution_binding_drift")
        env = await self.environments._resolve(row.request.environment,
                                                "stop" if cleanup else "inspect")
        spec = RuntimeSpec(env.record, row.request, execution.process_id, execution.execution_id)
        expected = self._descriptor(spec, execution.execution_id)
        if execution.attach_argv and expected.attach_argv != execution.attach_argv:
            raise EnvironmentError("execution_binding_drift")
        # Observation and host cleanup retain canonical bindings and their own
        # read/cleanup authority. An expired download grant must not hide exits;
        # prepare/arm and the broker still resolve current network permission.
        return spec

    @staticmethod
    def _status(spec: RuntimeSpec, value: dict[str, Any] | None) -> RuntimeStatus:
        assert spec.execution_id is not None
        if value is None:
            return RuntimeStatus(spec.execution_id, "unknown")
        state = docker.verify(spec, value)
        if state == "ready":
            return RuntimeStatus(spec.execution_id, "running")
        if state == "stopped":
            if value["State"]["Status"] == "created":
                return RuntimeStatus(spec.execution_id, "created")
            code = value["State"].get("ExitCode")
            if isinstance(code, int):
                return RuntimeStatus(spec.execution_id, "exited", code)
        return RuntimeStatus(spec.execution_id, "unknown")

    async def inspect(self, execution: RuntimeExecution) -> RuntimeStatus:
        handle = execution.environment
        async with self.environments.locks.hold(handle.owner_id, handle.project_id):
            spec = await self._bound(execution)
            return self._status(spec, await docker.inspect(self.runner, spec))

    async def cancel(self, execution: RuntimeExecution) -> RuntimeStatus:
        handle = execution.environment
        async with self.environments.locks.hold(handle.owner_id, handle.project_id):
            spec = await self._bound(execution, cleanup=True)
            await self.environments._network_release(execution.execution_id)
            value = await docker.inspect(self.runner, spec)
            if value is None:
                return RuntimeStatus(execution.execution_id, "unknown")
            docker.verify(spec, value)
            if value["State"]["Status"] == "created":
                # A create-only unit cannot be allowed to start after cancellation.
                await docker.checked(self.runner, ("rm", "--force", execution.execution_id))
                return RuntimeStatus(execution.execution_id, "exited")
            await docker.stop_verified(self.runner, spec, value)
            status = self._status(spec, await docker.inspect(self.runner, spec))
            if status.state != "exited":
                raise EnvironmentError("execution_stop_incomplete")
            # Retiring the container also invalidates an attach argv held by a
            # delayed C launch; docker start cannot resurrect a cancelled unit.
            await docker.checked(self.runner, ("rm", "--force", execution.execution_id))
            return status

    async def release(self, execution: RuntimeExecution) -> None:
        handle = execution.environment
        async with self.environments.locks.hold(handle.owner_id, handle.project_id):
            spec = await self._bound(execution, cleanup=True)
            await self.environments._network_release(execution.execution_id)
            value = await docker.inspect(self.runner, spec)
            if value is None:
                return
            docker.verify(spec, value)
            await docker.stop_verified(self.runner, spec, value)
            await docker.checked(self.runner, ("rm", "--force", execution.execution_id))
