from __future__ import annotations

import asyncio
import hashlib
from dataclasses import replace
from pathlib import Path

import pytest
from test_dev_environment_backend import FakeRunner, MemoryBindings, make_record

from krim_agent.dev_environment import DevelopmentBackend, EnvironmentError, ProjectLocks, docker
from krim_agent.dev_environment.model import RuntimeSpec


@pytest.fixture
def review_backend_env(tmp_path: Path):
    locks = tmp_path / "locks"
    locks.mkdir(mode=0o700)
    record = make_record(tmp_path / "project")
    bindings = MemoryBindings(record)
    runner = FakeRunner()
    runner.register_record(record)
    backend = DevelopmentBackend(
        bindings,
        ProjectLocks(locks, timeout_seconds=2),
        bindings.authorize,
        runner=runner,
        mount_check=lambda _spec: None,
    )
    return backend, bindings, runner, record


@pytest.mark.asyncio
async def test_stop_stops_bound_running_container_when_mount_check_fails(
    review_backend_env,
):
    backend, bindings, runner, original = review_backend_env
    container_id = "a" * 64
    ready = RuntimeSpec(original).observed("ready", container_id).record
    bindings.record = ready
    runner.add_container(ready, state="running", container_id=container_id)

    def reject_mount(_record) -> None:
        raise EnvironmentError("project_mount_drift")

    backend.mount_check = reject_mount

    stopped = await backend.stop(ready.environment)

    assert stopped.state == "stopped"
    assert runner.stop_calls == [container_id]
    assert bindings.authorized[-1] == "stop"


class PausedRunner(FakeRunner):
    @staticmethod
    def inspect_value(record, cid: str, state: str) -> dict:
        value = FakeRunner.inspect_value(record, cid, state)
        if state == "paused":
            value["State"].update(Paused=True, Running=False, Status="paused")
        return value

    async def run(self, args: tuple[str, ...]) -> docker.Reply:
        if args[:1] == ("unpause",):
            self.calls.append(args)
            cid = args[-1]
            record, _ = self.containers[cid]
            self.containers[cid] = (record, "running")
            return docker.Reply(0, "")
        return await super().run(args)


@pytest.mark.asyncio
async def test_stop_unpauses_bound_paused_container_before_stopping(
    tmp_path: Path,
):
    locks = tmp_path / "locks"
    locks.mkdir(mode=0o700)
    original = make_record(tmp_path / "project")
    container_id = "b" * 64
    ready = RuntimeSpec(original).observed("ready", container_id).record
    bindings = MemoryBindings(ready)
    runner = PausedRunner()
    runner.add_container(ready, state="paused", container_id=container_id)
    backend = DevelopmentBackend(
        bindings,
        ProjectLocks(locks),
        bindings.authorize,
        runner=runner,
        mount_check=lambda _spec: None,
    )

    stopped = await backend.stop(ready.environment)

    assert stopped.state == "stopped"
    assert runner.stop_calls == [container_id]
    assert runner.calls[-3:] == [
        ("unpause", container_id),
        ("stop", "--time=1", container_id),
        ("container", "inspect", container_id),
    ]


class RecreatePreflightRunner(FakeRunner):
    fail_image = False

    async def run(self, args: tuple[str, ...]) -> docker.Reply:
        if self.fail_image and args[:2] == ("image", "inspect"):
            self.calls.append(args)
            return docker.Reply(1, "", "image is unavailable")
        return await super().run(args)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["image", "network"])
async def test_recreate_preflight_failure_preserves_old_container(
    tmp_path: Path, failure: str
):
    locks = tmp_path / "locks"
    locks.mkdir(mode=0o700)
    project = tmp_path / "project"
    previous = make_record(project, state="ready", generation=1)
    old_id = hashlib.sha256(b"old generation").hexdigest()
    previous = replace(previous, container_id=old_id)
    current = make_record(project, state="creating", generation=2)
    if failure == "image":
        current = replace(current, image_id="sha256:" + "b" * 64)
    else:
        current = replace(
            current,
            network_id="c" * 64,
            network_policy_ref="policy-1",
        )

    bindings = MemoryBindings(current)
    runner = RecreatePreflightRunner()
    runner.add_container(previous, container_id=old_id)
    if failure == "image":
        runner.register_record(current)
        runner.fail_image = True
    else:
        runner.network_drift = True
    backend = DevelopmentBackend(
        bindings,
        ProjectLocks(locks),
        bindings.authorize,
        runner=runner,
        mount_check=lambda _spec: None,
    )

    with pytest.raises(EnvironmentError) as raised:
        await backend.recreate(current.environment, previous=previous)

    expected_error = {
        "image": "docker_image_failed",
        "network": "uncontrolled_network_forbidden",
    }[failure]
    assert raised.value.code == expected_error
    assert old_id in runner.containers
    assert runner.removed == []
    assert runner.create_attempts == 0


@pytest.mark.asyncio
async def test_docker_cli_kills_process_when_stdout_exceeds_limit(monkeypatch):
    class Stream:
        def __init__(self, chunks: list[bytes]) -> None:
            self.chunks = chunks

        async def read(self, _limit: int) -> bytes:
            if self.chunks:
                return self.chunks.pop(0)
            return b""

    class Process:
        def __init__(self) -> None:
            self.returncode: int | None = None
            self.stdout = Stream([b"x" * (4 * 1024 * 1024), b"x"])
            self.stderr = Stream([b""])
            self.killed = False

        def kill(self) -> None:
            self.killed = True

        async def wait(self) -> int:
            self.returncode = -9
            return self.returncode

    process = Process()

    async def spawn(*_args, **_kwargs):
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)

    with pytest.raises(EnvironmentError) as raised:
        await docker.DockerCLI().run(("start", "f" * 64))

    assert raised.value.code == "daemon_reply_too_large"
    assert process.killed
