from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from krim_agent.dev_environment import (
    ContainerExecutions,
    DevelopmentBackend,
    EnvironmentError,
    ProjectLocks,
    docker,
)
from krim_agent.dev_environment.model import RuntimeSpec
from krim_agent.development.contracts import (
    AuthorizationRef,
    CommandRequest,
    EnvironmentHandle,
    ProcessHandle,
    RunOwnership,
)
from krim_agent.development.ports import (
    EnvironmentRecord,
    ProcessRecord,
    ProcessReservation,
    RuntimeExecution,
    RuntimeStatus,
    command_digest,
)

IMAGE_ID = "sha256:" + "a" * 64
OWNER = "owner-1"
PROJECT = "project-1"
ENVIRONMENT = "environment-1"
SESSION = "session-1"
RUN = "run-1"


# These fakes deliberately live in this module. The environment backend tests
# are being migrated independently, and execution inspection must build the
# request-specific Docker shape rather than the environment-only /bin/sleep one.
def make_record(
    project_path: Path,
    *,
    generation: int = 1,
    state: str = "ready",
    container_id: str | None = None,
    network_id: str | None = None,
    network_policy_ref: str | None = None,
) -> EnvironmentRecord:
    project_path.mkdir(parents=True, exist_ok=True)
    metadata = project_path.stat()
    return EnvironmentRecord(
        environment=EnvironmentHandle(
            owner_id=OWNER,
            project_id=PROJECT,
            environment_id=ENVIRONMENT,
            generation=generation,
            state=state,
        ),
        image_id=IMAGE_ID,
        content_path=project_path,
        content_device=metadata.st_dev,
        content_inode=metadata.st_ino,
        container_id=container_id,
        network_id=network_id,
        network_policy_ref=network_policy_ref,
    )


def make_request(
    environment: EnvironmentHandle,
    *,
    operation_id: str = "operation-1",
    command: str = "printf 'ok'",
    cwd: str = ".",
    pty: bool = False,
) -> CommandRequest:
    ownership = RunOwnership(owner_id=environment.owner_id, session_id=SESSION, run_id=RUN)
    return CommandRequest(
        operation_id=operation_id,
        environment=environment,
        ownership=ownership,
        authorization=AuthorizationRef(
            actor_id="actor-1",
            actor_generation=1,
            project_id=environment.project_id,
            arguments_digest="b" * 64,
        ),
        command=command,
        cwd=cwd,
        pty=pty,
    )


def make_process(
    request: CommandRequest, process_id: str = "process-1", *, state: str = "accepted"
) -> ProcessRecord:
    handle = ProcessHandle(
        process_id=process_id,
        operation_id=request.operation_id,
        environment=request.environment,
        ownership=request.ownership,
        lifecycle=request.lifecycle,
        state=state,
        log_ref="log-1",
    )
    return ProcessRecord(request, handle, command_digest(request))


class MemoryEnvironmentRepository:
    def __init__(self, record: EnvironmentRecord) -> None:
        self.record = record
        self.authorized: list[str] = []
        self.saved: list[EnvironmentRecord] = []

    async def resolve(self, environment: EnvironmentHandle) -> EnvironmentRecord:
        del environment
        return self.record

    async def compare_and_swap(
        self, expected: EnvironmentRecord, updated: EnvironmentRecord
    ) -> EnvironmentRecord:
        if self.record != expected:
            raise EnvironmentError("binding_cas_conflict")
        if updated.revision != expected.revision + 1:
            raise EnvironmentError("input_revision_conflict")
        self.record = updated
        self.saved.append(updated)
        return updated

    async def authorize(self, environment: EnvironmentHandle, action: str) -> None:
        del environment
        self.authorized.append(action)


class MemoryProcessRepository:
    def __init__(self, *rows: ProcessRecord) -> None:
        self.rows = {row.handle.process_id: row for row in rows}
        self.get_calls: list[str] = []

    async def reserve(self, request: CommandRequest, request_digest: str) -> ProcessReservation:
        del request, request_digest
        raise AssertionError("prepare must consume an already-admitted process row")

    async def get(self, process_id: str) -> ProcessRecord:
        self.get_calls.append(process_id)
        return self.rows[process_id]

    async def save(self, expected: ProcessRecord, updated: ProcessRecord) -> ProcessRecord:
        current = self.rows[expected.handle.process_id]
        if current != expected:
            raise EnvironmentError("process_cas_conflict")
        if updated.revision != expected.revision + 1:
            raise EnvironmentError("input_revision_conflict")
        self.rows[updated.handle.process_id] = updated
        return updated

    async def list_environment(self, environment: EnvironmentHandle) -> tuple[ProcessRecord, ...]:
        identity = (
            environment.owner_id,
            environment.project_id,
            environment.environment_id,
            environment.generation,
        )
        return tuple(
            row
            for row in self.rows.values()
            if (
                row.request.environment.owner_id,
                row.request.environment.project_id,
                row.request.environment.environment_id,
                row.request.environment.generation,
            )
            == identity
        )

    async def list_run(self, ownership: RunOwnership) -> tuple[ProcessRecord, ...]:
        return tuple(row for row in self.rows.values() if row.request.ownership == ownership)

    async def list_environment_for_cleanup(
        self, previous: EnvironmentHandle,
    ) -> tuple[ProcessRecord, ...]:
        return await self.list_environment(previous)

    def persist_execution(self, process_id: str, execution_id: str) -> None:
        row = self.rows[process_id]
        self.rows[process_id] = replace(row, execution_id=execution_id, revision=row.revision + 1)

    def set_row(self, row: ProcessRecord) -> None:
        self.rows[row.handle.process_id] = row


class ExecutionRunner:
    prefix = ("/usr/bin/docker", "--host=unix:///var/run/docker.sock")

    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.specs_by_name: dict[str, RuntimeSpec] = {}
        self.containers: dict[str, tuple[RuntimeSpec, str, int | None]] = {}
        self.create_attempts = 0
        self.start_calls: list[str] = []
        self.stop_calls: list[str] = []
        self.removed: list[str] = []
        self.stop_exit_code = 23

    def register(self, spec: RuntimeSpec) -> None:
        self.specs_by_name[spec.name] = spec

    def add_container(
        self,
        spec: RuntimeSpec,
        *,
        state: str = "running",
        container_id: str | None = None,
        exit_code: int | None = None,
    ) -> str:
        self.register(spec)
        cid = container_id or hashlib.sha256(spec.name.encode()).hexdigest()
        self.containers[cid] = (spec, state, exit_code)
        return cid

    async def run(self, args: tuple[str, ...]) -> docker.Reply:
        self.calls.append(args)
        if args[:2] == ("container", "inspect"):
            target = args[2]
            found = next(
                (
                    (cid, spec, state, exit_code)
                    for cid, (spec, state, exit_code) in self.containers.items()
                    if cid == target or spec.name == target
                ),
                None,
            )
            if found is None:
                return docker.Reply(1, "", f"Error: No such container: {target}")
            cid, spec, state, exit_code = found
            return docker.Reply(0, json.dumps([self.inspect_value(spec, cid, state, exit_code)]))

        if args[:2] == ("container", "ls"):
            filters = [item[len("label=") :] for item in args if item.startswith("label=")]
            ids: list[str] = []
            for cid, (spec, _state, _exit_code) in self.containers.items():
                labels = docker.labels(spec)
                if all(
                    labels.get(value.split("=", 1)[0]) == value.split("=", 1)[1]
                    for value in filters
                ):
                    ids.append(cid)
            return docker.Reply(0, "\n".join(ids))

        if args[:2] == ("image", "inspect"):
            return docker.Reply(
                0,
                json.dumps([{"Id": args[2], "Config": {"Volumes": None}}]),
            )

        if args[:2] == ("network", "inspect"):
            network_id = args[2]
            spec = next(
                spec
                for spec in self.specs_by_name.values()
                if spec.network.network_id == network_id
            )
            return docker.Reply(
                0,
                json.dumps(
                    [
                        {
                            "Id": network_id,
                            "Name": "krim-development",
                            "Driver": "bridge",
                            "Labels": {docker.LABEL + "policy": spec.network.policy_ref},
                        }
                    ]
                ),
            )

        if args and args[0] == "create":
            self.create_attempts += 1
            name = args[args.index("--name") + 1]
            spec = self.specs_by_name[name]
            cid = hashlib.sha256(f"{name}:{self.create_attempts}".encode()).hexdigest()
            self.containers[cid] = (spec, "created", None)
            return docker.Reply(0, cid)

        if args and args[0] == "start":
            cid = args[-1]
            spec, _state, exit_code = self.containers[cid]
            self.containers[cid] = (spec, "running", exit_code)
            self.start_calls.append(cid)
            return docker.Reply(0, "")

        if args and args[0] == "stop":
            cid = args[-1]
            spec, _state, _exit_code = self.containers[cid]
            self.containers[cid] = (spec, "exited", self.stop_exit_code)
            self.stop_calls.append(cid)
            return docker.Reply(0, "")

        if args[:2] == ("rm", "--force"):
            cid = args[2]
            self.containers.pop(cid, None)
            self.removed.append(cid)
            return docker.Reply(0, cid)

        return docker.Reply(1, "", "unexpected fake Docker command")

    @staticmethod
    def inspect_value(
        spec: RuntimeSpec,
        cid: str,
        state: str,
        exit_code: int | None = None,
    ) -> dict[str, Any]:
        entrypoint, command = docker.command(spec)
        network_name = spec.network.network_id or "none"
        network_value = (
            {"NetworkID": spec.network.network_id}
            if spec.network.network_id is not None
            else {}
        )
        limits = spec.limits
        state_value: dict[str, Any] = {
            "Paused": False,
            "Restarting": False,
            "Dead": False,
            "Running": state == "running",
            "Status": state,
        }
        if state == "exited":
            state_value["ExitCode"] = exit_code
        return {
            "Id": cid,
            "Name": "/" + spec.name,
            "Image": spec.image_id,
            "Path": entrypoint,
            "Args": command,
            "Config": {
                "Image": spec.image_id,
                "Labels": docker.labels(spec),
                "User": docker.USER,
                "WorkingDir": "/workspace",
                "Entrypoint": [entrypoint],
                "Cmd": command,
                "Healthcheck": None,
                "Volumes": None,
                "OpenStdin": spec.request is not None,
                "Tty": spec.request is not None and spec.request.pty,
                "Env": [
                    "HOME=/home/krim",
                    "npm_config_cache=/workspace/.cache/npm",
                    "PIP_CACHE_DIR=/workspace/.cache/pip",
                ],
            },
            "HostConfig": {
                "Runtime": "runsc",
                "ReadonlyRootfs": True,
                "Privileged": False,
                "CapAdd": None,
                "CapDrop": ["ALL"],
                "SecurityOpt": ["no-new-privileges"],
                "PidMode": "",
                "IpcMode": "none" if spec.resource_policy == "unlimited" else "private",
                "UTSMode": "",
                "UsernsMode": "",
                "CgroupnsMode": "private",
                "Devices": [],
                "DeviceRequests": [],
                "DeviceCgroupRules": None,
                "Binds": None,
                "VolumesFrom": None,
                "Links": None,
                "PublishAllPorts": False,
                "PortBindings": None,
                "ExtraHosts": None,
                "Dns": None,
                "DnsSearch": None,
                "DnsOptions": None,
                "Sysctls": None,
                "RestartPolicy": {"Name": "no"},
                "LogConfig": {"Type": "none"},
                "NetworkMode": network_name,
                "Memory": limits.memory_bytes or 0,
                "MemorySwap": limits.memory_bytes or 0,
                "NanoCpus": limits.nano_cpus or 0,
                "PidsLimit": limits.pids,
                "ShmSize": (limits.shm_bytes if spec.legacy_profile
                            else 64 * 1024 * 1024),
                "Ulimits": ([{"Name": "nofile", "Soft": 1024, "Hard": 4096}]
                            if spec.legacy_profile else []),
                "OomKillDisable": False,
                "Init": False,
                "Tmpfs": docker.tmpfs(spec),
                "Mounts": [
                    {
                        "Type": "bind",
                        "Source": str(spec.mount.path),
                        "Target": "/workspace",
                        "ReadOnly": False,
                        "BindOptions": {
                            "Propagation": "rprivate",
                            "NonRecursive": True,
                        },
                    }
                ],
            },
            "Mounts": [
                {
                    "Type": "bind",
                    "Source": str(spec.mount.path),
                    "Destination": "/workspace",
                    "RW": True,
                    "Propagation": "rprivate",
                },
                *[
                    {"Type": "tmpfs", "Source": "tmpfs", "Destination": destination,
                     "RW": True, "Propagation": ""}
                    for destination in docker.tmpfs(spec)
                ],
            ],
            "NetworkSettings": {"Networks": {network_name: network_value}},
            "State": state_value,
        }


@pytest.fixture
def execution_env(tmp_path: Path):
    environment_id = hashlib.sha256(b"environment-container").hexdigest()
    record = make_record(tmp_path / "project", container_id=environment_id)
    bindings = MemoryEnvironmentRepository(record)
    runner = ExecutionRunner()
    runner.add_container(RuntimeSpec(record), container_id=environment_id)

    request = make_request(record.environment)
    process = make_process(request)
    processes = MemoryProcessRepository(process)
    runner.register(RuntimeSpec(record, request, process.handle.process_id))

    lock_root = tmp_path / "locks"
    lock_root.mkdir(mode=0o700)
    lock_root.chmod(0o700)
    backend = DevelopmentBackend(
        bindings,
        ProjectLocks(lock_root, timeout_seconds=2),
        bindings.authorize,
        runner=runner,
        mount_check=lambda _spec: None,
    )
    executions = ContainerExecutions(backend, processes)
    return executions, backend, bindings, processes, runner, record, request, process


async def prepare_and_persist(
    executions: ContainerExecutions,
    processes: MemoryProcessRepository,
    request: CommandRequest,
    process_id: str,
) -> RuntimeExecution:
    async with executions.prepare(request, process_id) as execution:
        processes.persist_execution(process_id, execution.execution_id)
    return execution


@pytest.mark.asyncio
async def test_prepare_creates_created_container_and_never_starts_it(execution_env):
    executions, _backend, _bindings, processes, runner, _record, request, process = execution_env

    async with executions.prepare(request, process.handle.process_id) as execution:
        assert execution.attach_argv[:5] == (
            "/usr/bin/env",
            "-i",
            "PATH=/usr/bin:/bin",
            "HOME=/nonexistent",
            "LC_ALL=C",
        )
        assert execution.attach_argv[5:] == (
            *runner.prefix,
            "start",
            "--attach",
            "--interactive",
            execution.execution_id,
        )
        processes.persist_execution(process.handle.process_id, execution.execution_id)

    assert runner.create_attempts == 1
    assert runner.start_calls == []
    assert runner.containers[execution.execution_id][1] == "created"
    create = next(call for call in runner.calls if call and call[0] == "create")
    assert "--user=65532:65532" in create
    assert "--network=none" in create


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["running", "exited", "cancelled", "failed", "unknown"])
async def test_prepare_requires_an_accepted_row_without_execution_id(execution_env, state: str):
    executions, _backend, _bindings, processes, _runner, _record, request, process = execution_env
    processes.set_row(
        replace(process, handle=process.handle.model_copy(update={"state": state}))
    )

    with pytest.raises(EnvironmentError) as raised:
        async with executions.prepare(request, process.handle.process_id):
            raise AssertionError("prepare must not yield")

    assert raised.value.code == "execution_already_prepared"


@pytest.mark.asyncio
async def test_prepare_rejects_row_that_already_has_a_receipt(execution_env):
    executions, _backend, _bindings, processes, _runner, _record, request, process = execution_env
    processes.set_row(replace(process, execution_id="a" * 64))

    with pytest.raises(EnvironmentError) as raised:
        async with executions.prepare(request, process.handle.process_id):
            raise AssertionError("prepare must not yield")

    assert raised.value.code == "execution_already_prepared"


@pytest.mark.asyncio
async def test_prepare_rejects_forged_request_digest(execution_env):
    executions, _backend, _bindings, processes, runner, _record, request, process = execution_env
    forged = request.model_copy(update={"command": "cat /etc/passwd"})

    with pytest.raises(EnvironmentError) as raised:
        async with executions.prepare(forged, process.handle.process_id):
            raise AssertionError("prepare must not yield")

    assert raised.value.code == "execution_binding_drift"
    assert runner.create_attempts == 0
    assert processes.rows[process.handle.process_id].execution_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize("forgery", ["owner", "generation"])
async def test_prepare_rejects_forged_owner_or_generation(execution_env, forgery: str):
    executions, _backend, _bindings, _processes, runner, _record, request, process = execution_env
    if forgery == "owner":
        environment = request.environment.model_copy(update={"owner_id": "other-owner"})
        ownership = request.ownership.model_copy(update={"owner_id": "other-owner"})
        forged = request.model_copy(update={"environment": environment, "ownership": ownership})
    else:
        environment = request.environment.model_copy(update={"generation": 2})
        forged = request.model_copy(update={"environment": environment})

    with pytest.raises(EnvironmentError) as raised:
        async with executions.prepare(forged, process.handle.process_id):
            raise AssertionError("prepare must not yield")

    assert raised.value.code == "environment_binding_drift"
    assert runner.create_attempts == 0


@pytest.mark.asyncio
async def test_prepare_rejects_forged_operation(execution_env):
    executions, _backend, _bindings, processes, runner, _record, request, process = execution_env
    forged = request.model_copy(update={"operation_id": "other-operation"})

    with pytest.raises(EnvironmentError) as raised:
        async with executions.prepare(forged, process.handle.process_id):
            raise AssertionError("prepare must not yield")

    assert raised.value.code == "execution_binding_drift"
    assert runner.create_attempts == 0
    assert processes.rows[process.handle.process_id].execution_id is None


@pytest.mark.asyncio
async def test_prepare_rejects_forged_process_id(execution_env):
    executions, _backend, _bindings, processes, runner, _record, request, process = execution_env
    processes.rows["forged-process"] = process

    with pytest.raises(EnvironmentError) as raised:
        async with executions.prepare(request, "forged-process"):
            raise AssertionError("prepare must not yield")

    assert raised.value.code == "execution_binding_drift"
    assert runner.create_attempts == 0


@pytest.mark.asyncio
async def test_prepare_rejects_missing_receipt_after_yield(execution_env):
    executions, _backend, _bindings, processes, runner, _record, request, process = execution_env

    with pytest.raises(EnvironmentError) as raised:
        async with executions.prepare(request, process.handle.process_id):
            pass

    assert raised.value.code == "execution_receipt_missing"
    assert runner.create_attempts == 1
    receipt = processes.rows[process.handle.process_id]
    assert receipt.handle.state == "cancelled"
    assert receipt.execution_id in runner.removed


@pytest.mark.asyncio
async def test_repeated_prepare_does_not_replay_creation(execution_env):
    executions, _backend, _bindings, processes, runner, _record, request, process = execution_env
    first = await prepare_and_persist(
        executions, processes, request, process.handle.process_id
    )

    with pytest.raises(EnvironmentError) as raised:
        async with executions.prepare(request, process.handle.process_id):
            raise AssertionError("a persisted receipt must prevent replay")

    assert raised.value.code == "execution_already_prepared"
    assert runner.create_attempts == 1
    assert first.execution_id in runner.containers


@pytest.mark.asyncio
async def test_existing_execution_without_receipt_is_unknown_and_not_relaunched(execution_env):
    executions, _backend, _bindings, processes, runner, record, request, process = execution_env
    spec = RuntimeSpec(record, request, process.handle.process_id)
    execution_id = hashlib.sha256(b"crash-left-execution").hexdigest()
    runner.add_container(spec, state="created", container_id=execution_id)

    with pytest.raises(EnvironmentError) as raised:
        async with executions.prepare(request, process.handle.process_id):
            raise AssertionError("an existing unit must not yield a launch descriptor")

    assert raised.value.code == "execution_outcome_unknown"
    assert runner.create_attempts == 0
    assert execution_id in runner.containers
    assert runner.start_calls == []
    assert processes.rows[process.handle.process_id].execution_id is None


@pytest.mark.asyncio
async def test_forged_operation_and_argv_are_rejected_after_receipt(execution_env):
    executions, _backend, _bindings, processes, _runner, _record, request, process = execution_env
    execution = await prepare_and_persist(
        executions, processes, request, process.handle.process_id
    )

    forged_operation = replace(execution, operation_id="other-operation")
    with pytest.raises(EnvironmentError) as raised:
        await executions.inspect(forged_operation)
    assert raised.value.code == "execution_binding_drift"

    forged_argv = replace(execution, attach_argv=execution.attach_argv + ("forged",))
    with pytest.raises(EnvironmentError) as raised:
        await executions.inspect(forged_argv)
    assert raised.value.code == "execution_binding_drift"


@pytest.mark.asyncio
async def test_inspect_missing_execution_is_unknown(execution_env):
    executions, _backend, _bindings, processes, runner, _record, request, process = execution_env
    execution = await prepare_and_persist(
        executions, processes, request, process.handle.process_id
    )
    runner.containers.pop(execution.execution_id)

    status = await executions.inspect(execution)

    assert status == RuntimeStatus(execution.execution_id, "unknown")


@pytest.mark.asyncio
async def test_cancel_created_removes_without_starting_and_has_no_exit_code(execution_env):
    executions, _backend, _bindings, processes, runner, _record, request, process = execution_env
    execution = await prepare_and_persist(
        executions, processes, request, process.handle.process_id
    )

    status = await executions.cancel(execution)

    assert status == RuntimeStatus(execution.execution_id, "exited")
    assert runner.stop_calls == []
    assert runner.start_calls == []
    assert runner.removed == [execution.execution_id]
    assert execution.execution_id not in runner.containers


@pytest.mark.asyncio
async def test_cancel_running_stops_and_removes_whole_container_before_delayed_attach(
    execution_env,
):
    executions, _backend, _bindings, processes, runner, _record, request, process = execution_env
    execution = await prepare_and_persist(
        executions, processes, request, process.handle.process_id
    )
    spec, _state, _exit_code = runner.containers[execution.execution_id]
    runner.containers[execution.execution_id] = (spec, "running", None)
    runner.stop_exit_code = 23

    status = await executions.cancel(execution)

    assert status == RuntimeStatus(execution.execution_id, "exited", 23)
    assert runner.stop_calls == [execution.execution_id]
    assert runner.removed == [execution.execution_id]
    assert runner.start_calls == []
    assert execution.execution_id not in runner.containers


@pytest.mark.asyncio
async def test_process_double_rejects_missing_revision_increment(execution_env):
    _executions, _backend, _bindings, processes, _runner, _record, _request, process = execution_env
    with pytest.raises(EnvironmentError, match="input_revision_conflict"):
        await processes.save(process, replace(process, execution_id="a" * 64))
    assert await processes.get(process.handle.process_id) == process


@pytest.mark.asyncio
async def test_terminal_receipt_increments_revision_exactly_once(execution_env):
    _executions, backend, _bindings, processes, _runner, _record, _request, process = execution_env
    await backend._finish_process(process, "cancelled", None, None)
    saved = await processes.get(process.handle.process_id)
    assert saved.handle.state == "cancelled"
    assert saved.revision == process.revision + 1
    await backend._finish_process(saved, "cancelled", None, None)
    assert await processes.get(process.handle.process_id) == saved


@pytest.mark.asyncio
async def test_release_is_idempotent_after_stopping_and_removing(execution_env):
    executions, _backend, _bindings, processes, runner, _record, request, process = execution_env
    execution = await prepare_and_persist(
        executions, processes, request, process.handle.process_id
    )
    spec, _state, _exit_code = runner.containers[execution.execution_id]
    runner.containers[execution.execution_id] = (spec, "running", None)

    await executions.release(execution)
    await executions.release(execution)

    assert runner.stop_calls == [execution.execution_id]
    assert runner.removed == [execution.execution_id]
    assert execution.execution_id not in runner.containers


@pytest.mark.asyncio
async def test_old_environment_generation_is_rejected_before_creation(execution_env):
    executions, _backend, bindings, _processes, runner, record, request, process = execution_env
    bindings.record = replace(
        record,
        environment=record.environment.model_copy(update={"generation": 2}),
    )

    with pytest.raises(EnvironmentError) as raised:
        async with executions.prepare(request, process.handle.process_id):
            raise AssertionError("a stale generation must not yield")

    assert raised.value.code == "environment_binding_drift"
    assert runner.create_attempts == 0


@pytest.mark.asyncio
async def test_prepare_holds_project_lock_until_receipt_handshake(execution_env):
    executions, backend, _bindings, processes, runner, record, request, process = execution_env

    async with executions.prepare(request, process.handle.process_id) as execution:
        before_stop = len(runner.calls)
        stop_task = asyncio.create_task(backend.stop(record.environment))
        await asyncio.sleep(0.05)
        assert not stop_task.done()
        assert len(runner.calls) == before_stop

        processes.persist_execution(process.handle.process_id, execution.execution_id)
        await asyncio.sleep(0.05)
        assert not stop_task.done()
        assert len(runner.calls) == before_stop

    stopped = await asyncio.wait_for(stop_task, timeout=1)

    assert stopped.state == "stopped"
    assert runner.removed == [execution.execution_id]
    assert runner.stop_calls == [record.container_id]


@pytest.mark.asyncio
async def test_default_execution_keeps_network_none_and_uid_65532(execution_env):
    executions, _backend, _bindings, processes, runner, _record, request, process = execution_env
    execution = await prepare_and_persist(
        executions, processes, request, process.handle.process_id
    )

    create = next(call for call in runner.calls if call and call[0] == "create")
    assert "--network=none" in create
    assert "--user=65532:65532" in create
    assert not any(call[:2] == ("network", "inspect") for call in runner.calls)
    spec, _state, _exit_code = runner.containers[execution.execution_id]
    value = runner.inspect_value(spec, execution.execution_id, "created")
    assert value["Config"]["User"] == "65532:65532"
    assert value["HostConfig"]["NetworkMode"] == "none"


@pytest.mark.asyncio
async def test_same_task_handshake_inspect_accepts_restored_empty_argv(execution_env):
    executions, _backend, _bindings, processes, _runner, _record, request, process = execution_env
    async with executions.prepare(request, process.handle.process_id) as execution:
        processes.persist_execution(process.handle.process_id, execution.execution_id)
        async with asyncio.timeout(1):
            status = await executions.inspect(replace(execution, attach_argv=()))
        assert status.state == "created"


@pytest.mark.asyncio
async def test_cancel_can_clean_created_unit_after_execution_id_cas_failure(execution_env):
    executions, _backend, _bindings, _processes, runner, _record, request, process = execution_env
    execution = None
    with pytest.raises(EnvironmentError, match="execution_receipt_missing"):
        async with executions.prepare(request, process.handle.process_id) as execution:
            pass
    assert execution is not None
    status = await executions.cancel(replace(execution, attach_argv=()))
    assert status.state == "unknown"  # prepare already persisted cancellation and removed it
    assert execution.execution_id not in runner.containers
