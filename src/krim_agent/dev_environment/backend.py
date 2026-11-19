"""Persistent project containers consuming I's shared repository and authority."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from krim_agent.dev_network.manager import DownloadNetworks

from krim_agent.development.contracts import EnvironmentHandle, ProcessHandle
from krim_agent.development.ports import (
    EnvironmentAuthorizer,
    EnvironmentRecord,
    EnvironmentRepository,
    ProcessRecord,
    ProcessRepository,
    command_digest,
)

from . import docker
from .locking import ProjectLocks, verify_mount
from .model import Action, EnvironmentError, RuntimeSpec, identity


class DevelopmentBackend:
    def __init__(self, repository: EnvironmentRepository, locks: ProjectLocks,
                 authorizer: EnvironmentAuthorizer, *, runner: docker.Runner | None = None,
                 mount_check: Callable[[RuntimeSpec], None] = verify_mount,
                 processes: ProcessRepository | None = None,
                 downloads: DownloadNetworks | None = None) -> None:
        self.repository = repository
        self.authorizer = authorizer
        self.locks = locks
        self.runner = runner or docker.DockerCLI()
        self.mount_check = mount_check
        self.processes = processes
        self.downloads = downloads

    async def _network_check(self, spec: RuntimeSpec) -> None:
        await docker.check_network(self.runner, spec)
        if spec.network.policy_ref is not None:
            if self.downloads is None:
                raise EnvironmentError("network_enforcement_required")
            await self.downloads.check(spec)

    async def _network_arm(self, spec: RuntimeSpec, cid: str) -> None:
        if spec.network.policy_ref is not None:
            if self.downloads is None:
                raise EnvironmentError("network_enforcement_required")
            await self.downloads.arm(spec, cid)

    async def _network_release(self, cid: str) -> None:
        if self.downloads is not None:
            await self.downloads.release(cid)

    async def _resolve(self, handle: EnvironmentHandle, action: Action) -> RuntimeSpec:
        record = await self.repository.resolve(handle)
        if identity(record.environment) != identity(handle):
            raise EnvironmentError("environment_binding_drift")
        spec = RuntimeSpec(record)
        if record.configuration_digest not in (None, spec.fingerprint):
            raise EnvironmentError("environment_configuration_drift")
        await self.authorizer(record.environment, action)
        if action != "stop":
            self.mount_check(spec)
        return spec

    async def _save(self, original: RuntimeSpec, state: str,
                    container: str | None) -> RuntimeSpec:
        updated = original.observed(state, container)
        record = replace(updated.record, configuration_digest=updated.fingerprint)
        if record == original.record:
            return original
        record = replace(record, revision=original.record.revision + 1)
        result = await self.repository.compare_and_swap(original.record, record)
        if result != record:
            raise EnvironmentError("environment_cas_invalid")
        return RuntimeSpec(result)

    async def _exclusive_container(self, record: RuntimeSpec, allowed: str | None) -> None:
        output = await docker.checked(self.runner, (
            "container", "ls", "--all", "--quiet", "--no-trunc",
            "--filter", "label=" + docker.LABEL + "owner=" + record.handle.owner_id,
            "--filter", "label=" + docker.LABEL + "project=" + record.handle.project_id,
            "--filter", "label=" + docker.LABEL + "kind=environment",
        ))
        if any(cid != allowed for cid in output.splitlines()):
            raise EnvironmentError("project_container_conflict")

    async def _observation(self, record: RuntimeSpec) -> tuple[str, str | None]:
        value = await docker.inspect(self.runner, record)
        if value is None:
            state = "creating" if (record.container_id is None
                                   and record.handle.state == "creating") else "unknown"
            return state, None
        return docker.verify(record, value), str(value["Id"])

    async def _ensure(self, record: RuntimeSpec, *, start: bool) -> EnvironmentHandle:
        if record.handle.state == "stopping":
            raise EnvironmentError("environment_stopping")
        await self._network_check(record)
        value = await docker.inspect(self.runner, record)
        if value is None:
            if record.container_id is not None or record.handle.state != "creating":
                await self._save(record, "unknown", record.container_id)
                raise EnvironmentError("environment_missing")
            await self._exclusive_container(record, None)
            await docker.check_image(self.runner, record)
            await self.authorizer(record.handle, "start" if start else "create")
            self.mount_check(record)
            await docker.checked(self.runner, docker.create_args(record))
            value = await docker.inspect(self.runner, record)
            if value is None:
                raise EnvironmentError("create_outcome_unknown")
        state = docker.verify(record, value)
        cid = str(value["Id"])
        await self._exclusive_container(record, cid)
        record = await self._save(record, state, cid)
        if start and state == "stopped":
            await self.authorizer(record.handle, "start")
            self.mount_check(record)
            await docker.checked(self.runner, ("start", cid))
            state, observed_id = await self._observation(record)
            if observed_id != cid:
                raise EnvironmentError("start_outcome_unknown")
            record = await self._save(record, state, cid)
        if start and state == "ready":
            await self._network_arm(record, cid)
            if record.network.policy_ref is not None:
                assert self.downloads is not None
                await self.downloads.wait_ready(cid)
        if start and record.handle.state != "ready":
            raise EnvironmentError("environment_not_ready")
        return record.handle

    async def _require_idle_execution_units(self, record: RuntimeSpec) -> None:
        if self.processes is not None:
            rows = await self.processes.list_environment(record.handle)
            if any(row.handle.state in {"accepted", "running", "unknown"} for row in rows):
                raise EnvironmentError("environment_migration_active_execution")
        output = await docker.checked(self.runner, (
            "container", "ls", "--all", "--quiet", "--no-trunc",
            "--filter", "label=" + docker.LABEL + "owner=" + record.handle.owner_id,
            "--filter", "label=" + docker.LABEL + "project=" + record.handle.project_id,
            "--filter", "label=" + docker.LABEL + "environment=" + record.handle.environment_id,
            "--filter", "label=" + docker.LABEL + "generation=" + str(record.handle.generation),
            "--filter", "label=" + docker.LABEL + "kind=execution",
        ))
        if output.splitlines():
            # A persisted row is not proof that an execution container is idle;
            # require the normal execution cleanup path to retire it first.
            raise EnvironmentError("environment_migration_execution_present")

    async def migrate_to_unlimited(self, environment: EnvironmentHandle) -> EnvironmentHandle:
        """Verify and replace a legacy idle container with an unlimited one.

        The project lock fences execution preparation. A legacy container is
        never reused under the new resource policy, and execution containers or
        active process receipts make migration fail closed.
        """
        async with self.locks.hold(environment.owner_id, environment.project_id):
            original = await self.repository.resolve(environment)
            if identity(original.environment) != identity(environment):
                raise EnvironmentError("environment_binding_drift")
            if original.environment.state == "stopping":
                raise EnvironmentError("environment_stopping")
            stored_policy = getattr(original, "resource_policy", "legacy")
            if (not isinstance(stored_policy, str)
                    or stored_policy not in {"unlimited", "legacy", "limited"}):
                raise EnvironmentError("environment_configuration_invalid")
            try:
                target_record = replace(original, resource_policy="unlimited")
            except TypeError:
                raise EnvironmentError("unlimited_policy_unavailable") from None
            target = RuntimeSpec(target_record)
            if original.configuration_digest == target.fingerprint:
                return await self._ensure(target, start=True)

            value = await docker.inspect(self.runner, target)
            old: RuntimeSpec | None = None
            old_state: str | None = None
            if value is None:
                if (original.container_id is None and original.environment.state == "creating"
                        and original.configuration_digest is None
                        and stored_policy == "unlimited"):
                    await self._require_idle_execution_units(target)
                    return await self._ensure(target, start=True)
                old = RuntimeSpec.legacy(original)
                if (original.configuration_digest is not None
                        and original.configuration_digest != old.fingerprint):
                    raise EnvironmentError("environment_configuration_drift")
            else:
                try:
                    state = docker.verify(target, value)
                except EnvironmentError:
                    old = RuntimeSpec.legacy(original)
                    old_state = docker.verify(old, value)
                    if (original.configuration_digest is not None
                            and original.configuration_digest not in {
                                old.fingerprint, target.fingerprint,
                            }):
                        raise EnvironmentError("environment_configuration_drift") from None
                else:
                    if state not in {"ready", "stopped"}:
                        raise EnvironmentError("environment_migration_state_unknown")
                    await self._require_idle_execution_units(target)
                    await self.authorizer(target.handle, "recreate")
                    cid = str(value["Id"])
                    updated = replace(
                        target_record,
                        environment=target_record.environment.model_copy(
                            update={"state": state}
                        ),
                        container_id=cid,
                        configuration_digest=target.fingerprint,
                        revision=original.revision + 1,
                    )
                    if updated != original:
                        saved = await self.repository.compare_and_swap(original, updated)
                        if saved != updated:
                            raise EnvironmentError("environment_cas_invalid")
                    else:
                        updated = original
                    return await self._ensure(RuntimeSpec(updated), start=True)

            if old is not None and old_state not in (None, "ready", "stopped"):
                raise EnvironmentError("environment_migration_state_unknown")
            await self._require_idle_execution_units(target)
            await self.authorizer(target.handle, "recreate")
            self.mount_check(target)
            container_to_remove = str(value["Id"]) if value is not None else None
            await self._exclusive_container(target, container_to_remove)
            transitioning = replace(
                target_record,
                environment=target_record.environment.model_copy(update={"state": "creating"}),
                container_id=None,
                configuration_digest=None,
                revision=original.revision + 1,
            )
            if transitioning != original:
                saved = await self.repository.compare_and_swap(original, transitioning)
                if saved != transitioning:
                    raise EnvironmentError("environment_cas_invalid")
            else:
                transitioning = original
            if container_to_remove is not None:
                # The prior unit was verified above. Clearing the durable pointer
                # first makes a crash/retry fail closed rather than reuse its limits.
                await docker.checked(self.runner, ("rm", "--force", container_to_remove))
            return await self._ensure(RuntimeSpec(transitioning), start=True)

    async def create(self, environment: EnvironmentHandle) -> EnvironmentHandle:
        async with self.locks.hold(environment.owner_id, environment.project_id):
            return await self._ensure(await self._resolve(environment, "create"), start=False)

    async def start(self, environment: EnvironmentHandle) -> EnvironmentHandle:
        async with self.locks.hold(environment.owner_id, environment.project_id):
            return await self._ensure(await self._resolve(environment, "start"), start=True)

    async def inspect(self, environment: EnvironmentHandle) -> EnvironmentHandle:
        async with self.locks.hold(environment.owner_id, environment.project_id):
            record = await self._resolve(environment, "inspect")
            await self._network_check(record)
            state, cid = await self._observation(record)
            await self._exclusive_container(record, cid)
            if record.handle.state == "stopping":
                # Observing an idle container cannot undo an unfinished cleanup fence.
                return record.handle
            return (await self._save(record, state, cid or record.container_id)).handle

    async def _finish_process(self, row: ProcessRecord, state: str,
                              exit_code: int | None, execution_id: str | None) -> None:
        assert self.processes is not None
        if row.handle.state in {"exited", "cancelled", "failed"}:
            return
        handle = ProcessHandle.model_validate({**row.handle.model_dump(),
                                              "state": state, "exit_code": exit_code})
        updated = replace(row, handle=handle, execution_id=execution_id, revision=row.revision + 1)
        saved = await self.processes.save(row, updated)
        if saved != updated:
            raise EnvironmentError("process_cas_invalid")

    async def _stop_executions(self, record: RuntimeSpec, *, retired: bool = False) -> None:
        """Stop verified units, persist terminal receipts, then remove containers.

        A CAS failure keeps the stopped unit and the environment's stopping fence
        for retry. Retired generations use I's restricted host-only enumeration.
        """
        output = await docker.checked(self.runner, (
            "container", "ls", "--all", "--quiet", "--no-trunc",
            "--filter", "label=" + docker.LABEL + "owner=" + record.handle.owner_id,
            "--filter", "label=" + docker.LABEL + "project=" + record.handle.project_id,
            "--filter", "label=" + docker.LABEL + "environment=" + record.handle.environment_id,
            "--filter", "label=" + docker.LABEL + "generation=" + str(record.handle.generation),
            "--filter", "label=" + docker.LABEL + "kind=execution",
        ))
        ids = set(output.splitlines())
        if self.processes is None:
            if ids:
                raise EnvironmentError("execution_repository_required")
            return
        rows = (await self.processes.list_environment_for_cleanup(record.handle) if retired
                else await self.processes.list_environment(record.handle))
        verified = []
        for row in rows:
            try:
                row.handle.require_binding(row.request)
            except ValueError:
                raise EnvironmentError("execution_binding_drift") from None
            if (identity(row.request.environment) != identity(record.handle)
                    or row.request_digest != command_digest(row.request)):
                raise EnvironmentError("execution_binding_drift")
            spec = RuntimeSpec(record.record, row.request, row.handle.process_id, row.execution_id)
            value = await docker.inspect(self.runner, spec)
            if value is not None:
                docker.verify(spec, value)
                ids.discard(str(value["Id"]))
                spec = replace(spec, execution_id=str(value["Id"]))
            verified.append((row, spec, value))
        if ids:
            raise EnvironmentError("execution_binding_missing")
        incomplete = False
        for row, spec, value in verified:
            if value is None:
                if row.handle.state in {"exited", "cancelled", "failed"}:
                    continue
                if row.handle.state == "accepted" and row.execution_id is None:
                    # Still under the launch lock: this intent never acquired a
                    # runtime unit, and stopping now prevents any later prepare.
                    await self._finish_process(row, "cancelled", None, None)
                else:
                    incomplete = True  # Missing outcome is not an invented exit.
                continue
            was_exited = value["State"]["Status"] == "exited"
            await self._network_release(str(value["Id"]))
            await docker.stop_verified(self.runner, spec, value)
            stopped = await docker.inspect(self.runner, spec)
            if stopped is None or docker.verify(spec, stopped) != "stopped":
                incomplete = True
                continue
            code = stopped["State"].get("ExitCode")
            code = code if stopped["State"]["Status"] == "exited" and type(code) is int else None
            state = "exited" if was_exited and code is not None else "cancelled"
            await self._finish_process(row, state, code, spec.execution_id)
            await docker.checked(self.runner, ("rm", "--force", str(spec.execution_id)))
        if incomplete:
            raise EnvironmentError("execution_cleanup_unknown")

    async def stop(self, environment: EnvironmentHandle) -> EnvironmentHandle:
        async with self.locks.hold(environment.owner_id, environment.project_id):
            record = await self._resolve(environment, "stop")
            record = await self._save(record, "stopping", record.container_id)
            await self._stop_executions(record)
            value = await docker.inspect(self.runner, record)
            if value is None:
                raise EnvironmentError("environment_stop_unknown")
            await self._network_release(str(value["Id"]))
            state = await docker.stop_verified(self.runner, record, value)
            if state != "stopped":
                raise EnvironmentError("environment_stop_unknown")
            return (await self._save(record, state, str(value["Id"]))).handle

    async def recreate(self, environment: EnvironmentHandle, *,
                       previous: EnvironmentRecord) -> EnvironmentHandle:
        async with self.locks.hold(environment.owner_id, environment.project_id):
            current = await self._resolve(environment, "recreate")
            prior = RuntimeSpec(previous)
            old_id, new_id = identity(prior.handle), identity(current.handle)
            if (old_id[:3] != new_id[:3] or new_id[3] != old_id[3] + 1
                    or prior.mount != current.mount or prior.container_id is None):
                raise EnvironmentError("recreate_binding_drift")
            old = await docker.inspect(self.runner, prior)
            if old is not None:
                docker.verify(prior, old)
                await self._exclusive_container(current, prior.container_id)
                await self._network_check(current)
                await docker.check_image(self.runner, current)
                if current.container_id is not None or current.handle.state != "creating":
                    raise EnvironmentError("recreate_binding_drift")
            await self.authorizer(current.handle, "recreate")
            await self._stop_executions(prior, retired=True)
            if old is not None:
                await self._network_release(prior.container_id)
                await docker.checked(self.runner, ("rm", "--force", prior.container_id))
            return await self._ensure(current, start=True)
