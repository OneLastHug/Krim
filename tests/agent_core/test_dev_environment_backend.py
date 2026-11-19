from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import pytest

from krim_agent.dev_environment import (
    DevelopmentBackend,
    EnvironmentError,
    ProjectLocks,
    docker,
    locking,
)
from krim_agent.dev_environment.model import RuntimeSpec
from krim_agent.development.contracts import EnvironmentHandle
from krim_agent.development.ports import EnvironmentRecord

IMAGE_ID = "sha256:" + "a" * 64
OWNER = "owner-1"
PROJECT = "project-1"
ENVIRONMENT = "environment-1"


def make_record(
    project_path: Path,
    *,
    generation: int = 1,
    state: str = "creating",
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
        network_id=network_id,
        network_policy_ref=network_policy_ref,
    )


class MemoryBindings:
    """Test-only I adapter with whole-record compare-and-swap."""

    def __init__(self, record: EnvironmentRecord) -> None:
        self.record = record
        self.denied_actions: set[str] = set()
        self.authorized: list[str] = []
        self.saved: list[EnvironmentRecord] = []
        self.fail_save = False

    async def resolve(self, environment: EnvironmentHandle) -> EnvironmentRecord:
        del environment
        return self.record

    async def compare_and_swap(
        self, expected: EnvironmentRecord, updated: EnvironmentRecord
    ) -> EnvironmentRecord:
        if self.fail_save or self.record != expected:
            raise EnvironmentError("binding_cas_conflict")
        if updated.revision != expected.revision + 1:
            raise EnvironmentError("input_revision_conflict")
        self.record = updated
        self.saved.append(updated)
        return updated

    async def authorize(self, environment: EnvironmentHandle, action: str) -> None:
        del environment
        self.authorized.append(action)
        if action in self.denied_actions:
            raise EnvironmentError("authorization_denied")


class FakeRunner:
    """Docker CLI fake returning the inspect shape used by DockerCLI consumers."""

    prefix = ("/usr/bin/docker", "--host=unix:///var/run/docker.sock")

    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.containers: dict[str, tuple[RuntimeSpec, str]] = {}
        self.records_by_name: dict[str, RuntimeSpec] = {}
        self.create_attempts = 0
        self.start_calls: list[str] = []
        self.stop_calls: list[str] = []
        self.removed: list[str] = []
        self.inspect_mutator: Callable[[dict], None] | None = None
        self.network_drift = False
        self.create_delay = 0.0

    def register_record(self, record: EnvironmentRecord | RuntimeSpec) -> None:
        spec = record if isinstance(record, RuntimeSpec) else RuntimeSpec(record)
        self.records_by_name[spec.name] = spec

    def add_container(
        self,
        record: EnvironmentRecord | RuntimeSpec,
        *,
        state: str = "running",
        container_id: str | None = None,
    ) -> str:
        spec = record if isinstance(record, RuntimeSpec) else RuntimeSpec(record)
        cid = container_id or hashlib.sha256(spec.name.encode()).hexdigest()
        self.register_record(spec)
        self.containers[cid] = (spec, state)
        return cid

    async def run(self, args: tuple[str, ...]) -> docker.Reply:
        self.calls.append(args)
        if args[:2] == ("container", "inspect"):
            target = args[2]
            found = next(
                (
                    (cid, spec, state)
                    for cid, (spec, state) in self.containers.items()
                    if cid == target or spec.name == target
                ),
                None,
            )
            if found is None:
                return docker.Reply(1, "", f"Error: No such container: {target}")
            cid, spec, state = found
            value = self.inspect_value(spec, cid, state)
            if self.inspect_mutator is not None:
                self.inspect_mutator(value)
            return docker.Reply(0, json.dumps([value]))

        if args[:2] == ("container", "ls"):
            filters = [item[len("label="):] for item in args if item.startswith("label=")]
            ids = []
            for cid, (spec, _state) in self.containers.items():
                labels = {
                    "owner": spec.handle.owner_id,
                    "project": spec.handle.project_id,
                    "environment": spec.handle.environment_id,
                    "generation": str(spec.handle.generation),
                    "kind": "execution" if spec.request is not None else "environment",
                }
                for value in filters:
                    key, expected = value.split("=", 1)
                    key = key.removeprefix(docker.LABEL)
                    if key not in labels or labels[key] != expected:
                        break
                else:
                    ids.append(cid)
            return docker.Reply(0, "\n".join(ids))

        if args[:2] == ("image", "inspect"):
            return docker.Reply(
                0,
                json.dumps([{"Id": args[2], "Config": {"Volumes": None}}]),
            )

        if args[:2] == ("network", "inspect"):
            network_id = args[2]
            policy = next(
                (
                    spec.network.policy_ref
                    for spec in self.records_by_name.values()
                    if spec.network.network_id == network_id
                ),
                "policy-1",
            )
            if self.network_drift:
                network_id = "b" * 64
            return docker.Reply(
                0,
                json.dumps(
                    [
                        {
                            "Id": network_id,
                            "Name": "krim-development",
                            "Driver": "bridge",
                            "Labels": {docker.LABEL + "policy": policy},
                        }
                    ]
                ),
            )

        if args[0] == "create":
            self.create_attempts += 1
            if self.create_delay:
                await asyncio.sleep(self.create_delay)
            name = args[args.index("--name") + 1]
            spec = self.records_by_name[name]
            cid = hashlib.sha256(f"{name}:{self.create_attempts}".encode()).hexdigest()
            self.containers[cid] = (spec, "created")
            return docker.Reply(0, cid)

        if args[0] == "start":
            cid = args[-1]
            spec, _ = self.containers[cid]
            self.containers[cid] = (spec, "running")
            self.start_calls.append(cid)
            return docker.Reply(0, "")

        if args[0] == "stop":
            cid = args[-1]
            spec, _ = self.containers[cid]
            self.containers[cid] = (spec, "exited")
            self.stop_calls.append(cid)
            return docker.Reply(0, "")

        if args[:2] == ("rm", "--force"):
            cid = args[2]
            self.containers.pop(cid, None)
            self.removed.append(cid)
            return docker.Reply(0, cid)

        return docker.Reply(1, "", "unexpected fake Docker command")

    @staticmethod
    def inspect_value(spec: RuntimeSpec, cid: str, state: str) -> dict:
        network_name = spec.network.network_id or "none"
        network_value = (
            {"NetworkID": spec.network.network_id}
            if spec.network.network_id is not None
            else {}
        )
        limits = spec.limits
        return {
            "Id": cid,
            "Name": "/" + spec.name,
            "Image": spec.image_id,
            "Path": "/bin/sleep",
            "Args": ["infinity"],
            "Config": {
                "Image": spec.image_id,
                "Labels": docker.labels(spec),
                "User": docker.USER,
                "WorkingDir": "/workspace",
                "Entrypoint": ["/bin/sleep"],
                "Cmd": ["infinity"],
                "Healthcheck": None,
                "Volumes": None,
                "OpenStdin": False,
                "Tty": False,
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
            "State": {
                "Paused": False,
                "Restarting": False,
                "Dead": False,
                "Running": state == "running",
                "Status": state,
            },
        }


@pytest.fixture
def backend_env(tmp_path: Path):
    lock_root = tmp_path / "locks"
    lock_root.mkdir(mode=0o700)
    lock_root.chmod(0o700)
    record = make_record(tmp_path / "project")
    bindings = MemoryBindings(record)
    runner = FakeRunner()
    runner.register_record(record)
    backend = DevelopmentBackend(
        bindings,
        ProjectLocks(lock_root, timeout_seconds=2),
        bindings.authorize,
        runner=runner,
        mount_check=lambda _spec: None,
    )
    return backend, bindings, runner, record


@pytest.mark.asyncio
async def test_create_start_stop_start_keep_generation_and_mount_without_exec(backend_env):
    backend, bindings, runner, original = backend_env

    created = await backend.create(original.environment)
    assert created.state == "stopped"
    ready = await backend.start(created)
    repeated = await backend.start(ready)
    stopped = await backend.stop(repeated)
    restarted = await backend.start(stopped)

    assert [created.generation, ready.generation, repeated.generation,
            stopped.generation, restarted.generation] == [1, 1, 1, 1, 1]
    assert bindings.record.content_path == original.content_path
    assert bindings.record.container_id is not None
    assert restarted.state == "ready"
    assert runner.create_attempts == 1
    assert len(runner.start_calls) == 2
    assert len(runner.stop_calls) == 1
    assert not any("exec" in args for args in runner.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("owner_id", "another-owner"),
        ("project_id", "another-project"),
        ("environment_id", "another-environment"),
        ("generation", 2),
    ],
)
async def test_resolve_rejects_identity_mismatch(backend_env, field: str, value):
    backend, _, _, original = backend_env
    mismatched = original.environment.model_copy(update={field: value})

    with pytest.raises(EnvironmentError) as raised:
        await backend.start(mismatched)

    assert raised.value.code == "environment_binding_drift"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift",
    ["image", "mount", "resources", "network", "uid"],
)
async def test_existing_container_configuration_drift_is_rejected(backend_env, drift: str):
    backend, bindings, runner, original = backend_env
    ready = RuntimeSpec(original).observed("ready", hashlib.sha256(b"container").hexdigest()).record
    bindings.record = ready
    runner.add_container(ready, container_id=ready.container_id)

    def mutate(value: dict) -> None:
        if drift == "image":
            value["Config"]["Image"] = "sha256:" + "b" * 64
        elif drift == "mount":
            value["Mounts"][0]["Source"] = "/different/project"
        elif drift == "resources":
            value["HostConfig"]["Memory"] += 1
        elif drift == "network":
            value["NetworkSettings"]["Networks"] = {"unexpected": {"NetworkID": "b" * 64}}
        elif drift == "uid":
            value["Config"]["User"] = "0:0"

    runner.inspect_mutator = mutate
    with pytest.raises(EnvironmentError) as raised:
        await backend.inspect(ready.environment)

    assert raised.value.code == "container_binding_drift"
    assert runner.create_attempts == 0


@pytest.mark.asyncio
async def test_dedicated_network_policy_drift_is_rejected(backend_env):
    backend, bindings, runner, original = backend_env
    network_id = "c" * 64
    record = replace(
        original,
        network_id=network_id,
        network_policy_ref="policy-1",
    )
    bindings.record = record
    with pytest.raises(EnvironmentError) as raised:
        await backend.inspect(record.environment)

    assert raised.value.code == "uncontrolled_network_forbidden"
    assert not runner.calls


@pytest.mark.asyncio
async def test_missing_known_container_becomes_unknown_without_recreation(backend_env):
    backend, bindings, runner, original = backend_env
    known_id = hashlib.sha256(b"previous container").hexdigest()
    bound = RuntimeSpec(original).observed("stopped", known_id).record
    bindings.record = bound

    with pytest.raises(EnvironmentError) as raised:
        await backend.start(bound.environment)

    assert raised.value.code == "environment_missing"
    assert bindings.record.environment.state == "unknown"
    assert bindings.record.container_id == known_id
    assert runner.create_attempts == 0


@pytest.mark.asyncio
async def test_failed_compare_and_swap_does_not_claim_created_state(backend_env):
    backend, bindings, runner, original = backend_env
    bindings.fail_save = True

    with pytest.raises(EnvironmentError) as raised:
        await backend.create(original.environment)

    assert raised.value.code == "binding_cas_conflict"
    assert runner.create_attempts == 1
    assert bindings.record == original


@pytest.mark.asyncio
async def test_authorization_denial_prevents_docker_mutation(backend_env):
    backend, bindings, runner, original = backend_env
    bindings.denied_actions.add("start")

    with pytest.raises(EnvironmentError) as raised:
        await backend.start(original.environment)

    assert raised.value.code == "authorization_denied"
    assert runner.calls == []
    assert runner.create_attempts == 0


def invalid_recreate_records(tmp_path: Path, *, generation: int, change_mount: bool):
    previous = make_record(tmp_path / "project", state="ready", generation=1)
    old_id = hashlib.sha256(b"old generation").hexdigest()
    previous = replace(previous, container_id=old_id)
    content_path = previous.content_path
    content_device = previous.content_device
    content_inode = previous.content_inode
    if change_mount:
        other = tmp_path / "other-project"
        other.mkdir()
        metadata = other.stat()
        content_path, content_device, content_inode = other, metadata.st_dev, metadata.st_ino
    current = EnvironmentRecord(
        environment=EnvironmentHandle(
            owner_id=OWNER,
            project_id=PROJECT,
            environment_id=ENVIRONMENT,
            generation=generation,
            state="creating",
        ),
        image_id=IMAGE_ID,
        content_path=content_path,
        content_device=content_device,
        content_inode=content_inode,
    )
    return previous, current, old_id


@pytest.mark.asyncio
async def test_recreate_removes_only_old_container_then_starts_next_generation(tmp_path: Path):
    lock_root = tmp_path / "locks"
    lock_root.mkdir(mode=0o700)
    previous, current, old_id = invalid_recreate_records(
        tmp_path, generation=2, change_mount=False
    )
    bindings = MemoryBindings(current)
    runner = FakeRunner()
    runner.add_container(previous, container_id=old_id)
    runner.register_record(current)
    backend = DevelopmentBackend(
        bindings,
        ProjectLocks(lock_root),
        bindings.authorize,
        runner=runner,
        mount_check=lambda _spec: None,
    )

    result = await backend.recreate(current.environment, previous=previous)

    assert result.state == "ready"
    assert result.generation == 2
    assert bindings.record.content_path == previous.content_path
    assert runner.removed == [old_id]
    assert runner.create_attempts == 1
    assert not any("exec" in args for args in runner.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("generation", "change_mount"),
    [(1, False), (3, False), (2, True)],
)
async def test_recreate_rejects_non_next_generation_or_changed_mount(
    tmp_path: Path, generation: int, change_mount: bool
):
    lock_root = tmp_path / "locks"
    lock_root.mkdir(mode=0o700)
    previous, current, old_id = invalid_recreate_records(
        tmp_path, generation=generation, change_mount=change_mount
    )
    bindings = MemoryBindings(current)
    runner = FakeRunner()
    runner.add_container(previous, container_id=old_id)
    backend = DevelopmentBackend(
        bindings,
        ProjectLocks(lock_root),
        bindings.authorize,
        runner=runner,
        mount_check=lambda _spec: None,
    )

    with pytest.raises(EnvironmentError) as raised:
        await backend.recreate(current.environment, previous=previous)

    assert raised.value.code == "recreate_binding_drift"
    assert runner.removed == []
    assert runner.create_attempts == 0


@pytest.mark.asyncio
async def test_concurrent_start_creates_only_one_container(backend_env):
    backend, bindings, runner, original = backend_env
    runner.create_delay = 0.05

    first, second = await asyncio.gather(
        backend.start(original.environment),
        backend.start(original.environment),
    )

    assert first.state == second.state == "ready"
    assert first.generation == second.generation == original.environment.generation
    assert first == second
    assert runner.create_attempts == 1
    assert len(bindings.saved) >= 2


def test_runtime_spec_derives_docker_binding_from_shared_record(tmp_path: Path):
    record = make_record(tmp_path / "project")
    spec = RuntimeSpec(record)
    observed = spec.observed("ready", "a" * 64)

    assert observed.record.environment.state == "ready"
    assert observed.record.container_id == "a" * 64
    assert (
        observed.handle.model_copy(update={"state": record.environment.state})
        == record.environment
    )
    assert observed.image_id == record.image_id
    assert observed.mount.path == record.content_path
    assert observed.limits.memory_bytes is None
    assert observed.limits.nano_cpus is None
    assert observed.limits.pids == -1
    assert observed.resource_policy == "unlimited"
    assert observed.network.network_id == record.network_id
    assert observed.name.startswith("krim-dev-")
    assert observed.fingerprint == RuntimeSpec(record).fingerprint


def test_real_mount_check_rejects_symlinked_project_path(tmp_path: Path):
    real_path = tmp_path / "real-project"
    real_path.mkdir()
    link_path = tmp_path / "project-link"
    link_path.symlink_to(real_path, target_is_directory=True)
    metadata = real_path.stat()
    record = make_record(real_path)
    record = replace(
        record,
        content_path=link_path,
        content_device=metadata.st_dev,
        content_inode=metadata.st_ino,
    )

    with pytest.raises(EnvironmentError) as raised:
        locking.verify_mount(RuntimeSpec(record))

    assert raised.value.code == "project_mount_drift"


@pytest.mark.asyncio
async def test_cancelled_lock_waiter_releases_file_descriptor_and_future_lock(backend_env):
    backend, _, runner, original = backend_env
    lock_root = backend.locks.root

    async with backend.locks.hold(OWNER, PROJECT):
        waiter = asyncio.create_task(backend.start(original.environment))
        await asyncio.sleep(0.06)
        assert not waiter.done()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

    result = await backend.start(original.environment)

    assert result.state == "ready"
    assert runner.create_attempts == 1
    assert lock_root.stat().st_mode & 0o777 == 0o700


@pytest.mark.asyncio
async def test_inspect_never_created_environment_preserves_create_intent(backend_env):
    backend, bindings, runner, original = backend_env
    inspected = await backend.inspect(original.environment)
    assert inspected.state == "creating"
    assert bindings.record.container_id is None
    assert (await backend.start(inspected)).state == "ready"
    assert runner.create_attempts == 1


@pytest.mark.asyncio
async def test_create_receipt_loss_recovers_same_idle_container(backend_env):
    backend, bindings, runner, original = backend_env
    bindings.fail_save = True
    with pytest.raises(EnvironmentError):
        await backend.create(original.environment)
    bindings.fail_save = False
    ready = await backend.start(original.environment)
    assert ready.state == "ready"
    assert runner.create_attempts == 1
    assert len(runner.start_calls) == 1
    assert not any("exec" in args for args in runner.calls)


@pytest.mark.asyncio
async def test_project_lock_serializes_independent_process(tmp_path: Path):
    import sys

    locks = tmp_path / "locks"
    locks.mkdir(mode=0o700)
    script = """import asyncio,sys
from pathlib import Path
from krim_agent.dev_environment import ProjectLocks
async def main():
 print('waiting', flush=True)
 async with ProjectLocks(Path(sys.argv[1])).hold('owner', 'project'):
  print('acquired', flush=True)
asyncio.run(main())
"""
    async with ProjectLocks(locks).hold("owner", "project"):
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-c", script, str(locks),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        assert await process.stdout.readline() == b"waiting\n"
        output = asyncio.create_task(process.stdout.readline())
        await asyncio.sleep(0.06)
        assert not output.done()
    assert await asyncio.wait_for(output, 5) == b"acquired\n"
    assert await asyncio.wait_for(process.wait(), 5) == 0


@pytest.mark.asyncio
async def test_docker_cancellation_settles_pending_daemon_command(monkeypatch):
    finished = asyncio.Event()
    entered = asyncio.Event()

    class Stream:
        async def read(self, limit):
            entered.set()
            await finished.wait()
            return b""

    class Process:
        returncode = None
        stdout = Stream()
        stderr = Stream()

        async def wait(self):
            await finished.wait()
            self.returncode = 0
            return 0

    process = Process()

    async def spawn(*args, **kwargs):
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    task = asyncio.create_task(docker.DockerCLI().run(("start", "c" * 64)))
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0.02)
    assert not task.done()
    finished.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert process.returncode == 0


@pytest.mark.asyncio
async def test_environment_writes_increment_revision_once_and_skip_noop(backend_env):
    backend, bindings, _runner, original = backend_env
    ready = await backend.start(original.environment)
    assert [row.revision for row in bindings.saved] == [original.revision + 1,
                                                       original.revision + 2]
    last = bindings.record
    await backend.start(ready)
    await backend.inspect(ready)
    assert bindings.record == last
    assert len(bindings.saved) == 2


@pytest.mark.asyncio
async def test_environment_double_rejects_missing_or_skipped_revision(backend_env):
    _backend, bindings, _runner, original = backend_env
    for revision in (original.revision, original.revision + 2):
        with pytest.raises(EnvironmentError, match="input_revision_conflict"):
            await bindings.compare_and_swap(original, replace(original, revision=revision))
    assert bindings.record == original
