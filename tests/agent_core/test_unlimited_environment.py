from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from krim_agent.dev_environment import DevelopmentBackend, EnvironmentError, ProjectLocks, docker
from krim_agent.dev_environment.model import RuntimeSpec
from krim_agent.development.contracts import EnvironmentHandle
from krim_agent.development.ports import EnvironmentRecord

IMAGE_ID = "sha256:" + "a" * 64
OWNER = "owner-1"
PROJECT = "project-1"
ENVIRONMENT = "environment-1"


def make_record(path: Path, *, state: str = "creating", **changes: Any) -> EnvironmentRecord:
    path.mkdir(parents=True, exist_ok=True)
    stat = path.stat()
    record = EnvironmentRecord(
        environment=EnvironmentHandle(
            owner_id=OWNER,
            project_id=PROJECT,
            environment_id=ENVIRONMENT,
            generation=1,
            state=state,
        ),
        image_id=IMAGE_ID,
        content_path=path,
        content_device=stat.st_dev,
        content_inode=stat.st_ino,
    )
    return replace(record, **changes) if changes else record


def inspect_value(spec: RuntimeSpec, cid: str, state: str) -> dict[str, Any]:
    entrypoint, command = docker.command(spec)
    limits = spec.limits
    legacy = spec.legacy_profile
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
            "IpcMode": "private" if legacy else "none",
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
            "NetworkMode": "none",
            "Memory": limits.memory_bytes or 0,
            "MemorySwap": limits.memory_bytes or 0,
            "NanoCpus": limits.nano_cpus or 0,
            "PidsLimit": limits.pids,
            "ShmSize": limits.shm_bytes if legacy else 64 * 1024 * 1024,
            "Ulimits": ([{"Name": "nofile", "Soft": 1024, "Hard": 4096}]
                        if legacy else []),
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
        "NetworkSettings": {"Networks": {"none": {}}},
        "State": {
            "Paused": False,
            "Restarting": False,
            "Dead": False,
            "Running": state == "running",
            "Status": state,
        },
    }


class MemoryEnvironmentRepository:
    def __init__(self, record: EnvironmentRecord) -> None:
        self.record = record

    async def resolve(self, environment: EnvironmentHandle) -> EnvironmentRecord:
        del environment
        return self.record

    async def compare_and_swap(
        self, expected: EnvironmentRecord, updated: EnvironmentRecord,
    ) -> EnvironmentRecord:
        if self.record != expected or updated.revision != expected.revision + 1:
            raise EnvironmentError("binding_cas_conflict")
        self.record = updated
        return updated


class FakeRunner:
    prefix = ("/usr/bin/docker", "--host=unix:///var/run/docker.sock")

    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.specs: dict[str, RuntimeSpec] = {}
        self.containers: dict[str, tuple[RuntimeSpec, str]] = {}
        self.create_count = 0
        self.removed: list[str] = []
        self.start_calls: list[str] = []

    def register(self, spec: RuntimeSpec) -> None:
        self.specs[spec.name] = spec

    def add_container(self, spec: RuntimeSpec, cid: str, state: str = "running") -> None:
        self.register(spec)
        self.containers[cid] = spec, state

    async def run(self, args: tuple[str, ...]) -> docker.Reply:
        self.calls.append(args)
        if args[:2] == ("container", "inspect"):
            target = args[2]
            found = next(((cid, spec, state) for cid, (spec, state) in self.containers.items()
                          if cid == target or spec.name == target), None)
            if found is None:
                return docker.Reply(1, "", f"Error: No such container: {target}")
            cid, spec, state = found
            return docker.Reply(0, json.dumps([inspect_value(spec, cid, state)]))
        if args[:2] == ("container", "ls"):
            filters = [item.removeprefix("label=") for item in args if item.startswith("label=")]
            ids = []
            for cid, (spec, _state) in self.containers.items():
                labels = {key.removeprefix(docker.LABEL): value
                          for key, value in docker.labels(spec).items()}
                if all(labels.get(key) == expected
                       for key, expected in (item.split("=", 1) for item in filters)):
                    ids.append(cid)
            return docker.Reply(0, "\n".join(ids))
        if args[:2] == ("image", "inspect"):
            return docker.Reply(0, json.dumps([{"Id": args[2], "Config": {"Volumes": None}}]))
        if args[0] == "create":
            self.create_count += 1
            name = args[args.index("--name") + 1]
            spec = self.specs[name]
            cid = hashlib.sha256(f"{name}:{self.create_count}".encode()).hexdigest()
            self.containers[cid] = spec, "created"
            return docker.Reply(0, cid)
        if args[0] == "start":
            cid = args[-1]
            spec, _state = self.containers[cid]
            self.containers[cid] = spec, "running"
            self.start_calls.append(cid)
            return docker.Reply(0, "")
        if args[:2] == ("rm", "--force"):
            cid = args[2]
            self.containers.pop(cid, None)
            self.removed.append(cid)
            return docker.Reply(0, cid)
        return docker.Reply(1, "", "unexpected fake Docker command")


class ActiveProcesses:
    def __init__(self, state: str) -> None:
        self.state = state

    async def list_environment(self, environment: EnvironmentHandle):
        del environment
        return (type("Row", (), {"handle": type("Handle", (), {"state": self.state})()})(),)


def backend_for(tmp_path: Path, record: EnvironmentRecord, runner: FakeRunner,
                processes: Any = None) -> tuple[DevelopmentBackend, MemoryEnvironmentRepository]:
    lock_root = tmp_path / "locks"
    lock_root.mkdir(mode=0o700)
    repository = MemoryEnvironmentRepository(record)
    backend = DevelopmentBackend(
        repository,
        ProjectLocks(lock_root),
        lambda _environment, _action: _allow(),
        runner=runner,
        mount_check=lambda _spec: None,
        processes=processes,
    )
    return backend, repository


async def _allow() -> None:
    return None


def test_unlimited_spec_omits_caps_and_uses_unbounded_tmpfs(tmp_path: Path):
    spec = RuntimeSpec(make_record(tmp_path / "project"))
    args = docker.create_args(spec)

    assert "--pids-limit=-1" in args
    assert "--ipc=none" in args
    assert not any(arg.startswith(("--memory=", "--memory-swap=", "--cpus=",
                                   "--shm-size=", "--ulimit=")) for arg in args)
    mounts = docker.tmpfs(spec)
    assert set(mounts) == {"/tmp", "/home/krim", "/dev/shm"}
    assert all("size=0" in options and "nr_inodes=0" in options
               for options in mounts.values())
    value = inspect_value(spec, "a" * 64, "running")
    assert docker.verify(spec, value) == "ready"
    assert value["HostConfig"]["IpcMode"] == "none"
    assert "/dev/shm" in value["HostConfig"]["Tmpfs"]
    assert any(mount["Destination"] == "/dev/shm" and mount["Type"] == "tmpfs"
               for mount in value["Mounts"])


def test_unlimited_verification_accepts_daemon_null_pid_and_tmpfs_inventory(tmp_path: Path):
    spec = RuntimeSpec(make_record(tmp_path / "project"))
    value = inspect_value(spec, "a" * 64, "running")
    value["HostConfig"]["PidsLimit"] = None
    value["Mounts"] = [mount for mount in value["Mounts"] if mount["Type"] == "bind"]
    assert docker.verify(spec, value) == "ready"


def test_limited_pid_limit_cannot_be_replaced_by_daemon_null(tmp_path: Path):
    spec = RuntimeSpec(make_record(
        tmp_path / "project", resource_policy="limited", pids_limit=256,
    ))
    value = inspect_value(spec, "a" * 64, "running")
    value["HostConfig"]["PidsLimit"] = None
    with pytest.raises(EnvironmentError, match="container_binding_drift"):
        docker.verify(spec, value)


def test_unlimited_verification_rejects_residual_limits_and_shm_drift(tmp_path: Path):
    spec = RuntimeSpec(make_record(tmp_path / "project"))
    mutations = (
        lambda value: value["HostConfig"].__setitem__("PidsLimit", 256),
        lambda value: value["HostConfig"].__setitem__("IpcMode", "private"),
        lambda value: value["HostConfig"]["Tmpfs"].__setitem__(
            "/dev/shm", "rw,nosuid,nodev,size=67108864,mode=1777"
        ),
        lambda value: value["Mounts"].remove(next(
            mount for mount in value["Mounts"] if mount["Destination"] == "/dev/shm"
        )),
    )
    for mutate in mutations:
        value = inspect_value(spec, "a" * 64, "running")
        mutate(value)
        with pytest.raises(EnvironmentError, match="container_binding_drift"):
            docker.verify(spec, value)


def test_limited_policy_only_applies_each_non_null_limit(tmp_path: Path):
    record = make_record(
        tmp_path / "project",
        resource_policy="limited",
        memory_bytes=256 * 1024 * 1024,
        cpu_cores=None,
        pids_limit=None,
    )
    spec = RuntimeSpec(record)
    args = docker.create_args(spec)

    assert "--memory=268435456" in args
    assert "--memory-swap=268435456" in args
    assert not any(arg.startswith("--cpus=") for arg in args)
    assert "--pids-limit=-1" in args
    assert "--ipc=none" in args
    assert all("size=0" in options for options in docker.tmpfs(spec).values())
    assert docker.verify(spec, inspect_value(spec, "b" * 64, "running")) == "ready"


def test_limited_policy_accepts_configured_values_without_legacy_ceilings(tmp_path: Path):
    record = make_record(
        tmp_path / "project",
        resource_policy="limited",
        cpu_cores=128,
        memory_bytes=6 * 1024 * 1024,
        pids_limit=100_000,
    )
    spec = RuntimeSpec(record)
    args = docker.create_args(spec)

    assert "--cpus=128.000000000" in args
    assert "--memory=6291456" in args
    assert "--memory-swap=6291456" in args
    assert "--pids-limit=100000" in args
    assert docker.verify(spec, inspect_value(spec, "d" * 64, "running")) == "ready"


def test_explicit_legacy_spec_reconstructs_previous_bounded_container(tmp_path: Path):
    record = make_record(
        tmp_path / "project",
        cpu_cores=1.0,
        memory_bytes=1024 * 1024 * 1024,
        pids_limit=256,
    )
    old = RuntimeSpec.legacy(record)
    assert old.legacy_profile
    assert docker.verify(old, inspect_value(old, "c" * 64, "running")) == "ready"
    args = docker.create_args(old)
    assert "--memory=1073741824" in args
    assert "--cpus=1.000000000" in args
    assert "--pids-limit=256" in args
    assert "/dev/shm" not in docker.tmpfs(old)


def test_null_unknown_policy_or_legacy_resources_fail_closed(tmp_path: Path):
    record = make_record(tmp_path / "project")
    with pytest.raises(EnvironmentError, match="environment_configuration_invalid"):
        RuntimeSpec(replace(record, resource_policy=None))  # type: ignore[arg-type]
    with pytest.raises(EnvironmentError, match="environment_configuration_invalid"):
        RuntimeSpec(replace(record, resource_policy="mystery"))  # type: ignore[arg-type]
    with pytest.raises(EnvironmentError, match="environment_configuration_invalid"):
        RuntimeSpec.legacy(record)


@pytest.mark.asyncio
async def test_migration_verifies_then_replaces_legacy_idle_container(tmp_path: Path):
    cid = "d" * 64
    record = make_record(
        tmp_path / "project",
        state="ready",
        container_id=cid,
        cpu_cores=1.0,
        memory_bytes=1024 * 1024 * 1024,
        pids_limit=256,
    )
    old = RuntimeSpec.legacy(record)
    record = replace(record, configuration_digest=old.fingerprint)
    old = RuntimeSpec.legacy(record)
    target = RuntimeSpec(record)
    runner = FakeRunner()
    runner.add_container(old, cid)
    runner.register(target)
    backend, repository = backend_for(tmp_path, record, runner)

    result = await backend.migrate_to_unlimited(record.environment)

    assert result.state == "ready"
    assert runner.removed == [cid]
    assert runner.create_count == 1
    assert len(runner.start_calls) == 1
    assert repository.record.configuration_digest == RuntimeSpec(repository.record).fingerprint
    assert RuntimeSpec(repository.record).resource_policy == "unlimited"
    create = next(call for call in runner.calls if call and call[0] == "create")
    assert "--pids-limit=-1" in create
    assert not any(arg.startswith("--memory=") or arg.startswith("--cpus=") for arg in create)


@pytest.mark.asyncio
async def test_migration_refuses_active_execution_without_deleting_environment(tmp_path: Path):
    cid = "e" * 64
    record = make_record(
        tmp_path / "project",
        state="ready",
        container_id=cid,
        configuration_digest="f" * 64,
        cpu_cores=1.0,
        memory_bytes=1024 * 1024 * 1024,
        pids_limit=256,
    )
    old = RuntimeSpec.legacy(record)
    record = replace(record, configuration_digest=old.fingerprint)
    old = RuntimeSpec.legacy(record)
    runner = FakeRunner()
    runner.add_container(old, cid)
    runner.register(RuntimeSpec(record))
    processes = ActiveProcesses("running")
    backend, repository = backend_for(tmp_path, record, runner, processes)

    with pytest.raises(EnvironmentError, match="environment_migration_active_execution"):
        await backend.migrate_to_unlimited(record.environment)

    assert repository.record == record
    assert runner.removed == []
    assert runner.create_count == 0
    assert cid in runner.containers


@pytest.mark.asyncio
async def test_migration_does_not_assume_null_legacy_limits_are_unlimited(tmp_path: Path):
    cid = "f" * 64
    bounded = make_record(
        tmp_path / "project",
        state="ready",
        container_id=cid,
        cpu_cores=1.0,
        memory_bytes=1024 * 1024 * 1024,
        pids_limit=256,
    )
    old = RuntimeSpec.legacy(bounded)
    unknown = replace(
        bounded,
        cpu_cores=None,
        memory_bytes=None,
        pids_limit=None,
        configuration_digest=old.fingerprint,
    )
    runner = FakeRunner()
    runner.add_container(old, cid)
    runner.register(RuntimeSpec(unknown))
    backend, repository = backend_for(tmp_path, unknown, runner)

    with pytest.raises(EnvironmentError, match="environment_configuration_invalid"):
        await backend.migrate_to_unlimited(unknown.environment)

    assert repository.record == unknown
    assert runner.removed == []
    assert runner.create_count == 0
    assert cid in runner.containers
