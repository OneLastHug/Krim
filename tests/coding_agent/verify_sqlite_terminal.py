"""Opt-in real I SQLite + B runsc + C host-supervisor integration.

Run as the trusted Linux host operator with an already installed approved image.
No package installation, external API, Git commit/push or deployment is performed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import tempfile
from dataclasses import replace
from pathlib import Path

from krim_agent.contracts import ServiceError
from krim_agent.dev_environment.backend import DevelopmentBackend
from krim_agent.dev_environment.docker import DockerCLI, Reply
from krim_agent.dev_environment.execution import ContainerExecutions
from krim_agent.dev_environment.locking import ProjectLocks
from krim_agent.dev_terminal.backend import TerminalBackend
from krim_agent.dev_terminal.client import WorkerClient
from krim_agent.dev_terminal.host import HostWorkerConnection
from krim_agent.development.contracts import CommandRequest, EnvironmentHandle, ProcessHandle
from krim_agent.storage.development import DevelopmentStore
from krim_agent.storage.sqlite import SQLiteStore
from tests.coding_agent.test_development_storage import prepare


class RecordingDocker(DockerCLI):
    """Track only the exact container IDs returned by this test's create calls."""

    def __init__(self, evidence: Path) -> None:
        self.created: list[str] = []
        self.evidence = evidence

    async def run(self, args: tuple[str, ...]) -> Reply:
        result = await super().run(args)
        if args and args[0] == "create" and result.code == 0:
            container = result.stdout.strip()
            if not re.fullmatch(r"[0-9a-f]{64}", container):
                raise ValueError("create did not return a full container ID")
            self.created.append(container)
            self.evidence.write_text(json.dumps(self.created, indent=2))
        return result


def directories(root: Path) -> tuple[Path, Path, Path, Path]:
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    sandbox_root = Path(
        tempfile.mkdtemp(prefix="coding-agent-integration-", dir="/srv/krim-sandbox")
    )
    content = sandbox_root / "content"
    content.mkdir(mode=0o700)
    os.chown(content, 65532, 65532)
    private = root / "supervisor"
    private.mkdir(mode=0o700)
    placeholder = root / "unused-project"
    placeholder.mkdir(mode=0o700)
    locks = root / "locks"
    locks.mkdir(mode=0o700)
    return content, private, placeholder, locks


async def main(root: Path, image: str) -> None:
    content, private, placeholder, locks = await asyncio.to_thread(directories, root)
    database = root / "application.sqlite"
    store, repositories, environment_record, template = await prepare(
        database,
        image_id=image,
        creating=True,
        content_path=content,
    )
    observations = []
    worker_output = (root / "supervisor.log").open("wb")
    supervisor = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "krim_agent.dev_terminal.worker",
        "serve",
        "--state",
        str(private),
        "--project",
        str(placeholder),
        "--trusted-attachments",
        stdout=worker_output,
        stderr=asyncio.subprocess.STDOUT,
    )
    docker = RecordingDocker(root / "created-container-ids.json")
    backend = None
    environment = template.environment

    def evidence(label: str, **values: object) -> None:
        observations.append({"step": label, **values})
        (root / "results.json").write_text(
            json.dumps(
                {
                    "image": image,
                    "content": str(content),
                    "observations": observations,
                    "full_project_acceptance": False,
                },
                indent=2,
            )
        )
        print(json.dumps(observations[-1]), flush=True)

    async def environment_authority(handle: EnvironmentHandle, action: str) -> None:
        current = await repositories.environments.resolve(handle)
        if current.environment.owner_id != template.ownership.owner_id:
            raise ServiceError("permission_denied")
        if action == "execute" and current.environment.state != "ready":
            raise ServiceError("workspace_busy")

    async def terminal_authority(request: CommandRequest, action: str) -> None:
        if request.ownership != template.ownership:
            raise ServiceError("permission_denied")
        await repositories.environments.resolve(request.environment)
        if action == "submit":
            snapshot = await store.snapshot(request.ownership.run_id)
            if snapshot.run.stop_requested:
                raise ServiceError("cancelled")

    def compose() -> TerminalBackend:
        nonlocal backend
        backend = DevelopmentBackend(
            repositories.environments,
            ProjectLocks(locks),
            environment_authority,
            processes=repositories.processes,
            runner=docker,
        )
        executions = ContainerExecutions(backend, repositories.processes)
        return TerminalBackend(
            repositories.processes,
            executions,
            WorkerClient(HostWorkerConnection(private)),
            authorize=terminal_authority,
        )

    async def done(terminal: TerminalBackend, process: ProcessHandle) -> ProcessHandle:
        async with asyncio.timeout(20):
            while process.state in {"accepted", "running"}:
                process = await terminal.inspect(process)
                await asyncio.sleep(0.05)
        return process

    try:
        async with asyncio.timeout(20):
            while not (private / "control.sock").exists():
                if supervisor.returncode is not None:
                    raise RuntimeError("supervisor failed to start")
                await asyncio.sleep(0.05)
        terminal = compose()
        assert backend is not None
        environment = await backend.start(environment)
        assert environment.state == "ready"
        template = template.model_copy(update={"environment": environment})
        evidence("environment-start", state=environment.state)
        request = template.model_copy(
            update={
                "command": "printf 'one\\n' >> effect.txt; printf sqlite-runsc-ok",
                "wait_seconds": 2.0,
            }
        )
        process = await done(terminal, await terminal.submit(request))
        assert process.state == "exited" and process.exit_code == 0
        output = await terminal.read_output(process, 0)
        assert "sqlite-runsc-ok" in output.text
        assert (content / "effect.txt").read_text() == "one\n"
        repeated = await terminal.submit(request)
        assert repeated.process_id == process.process_id
        assert (content / "effect.txt").read_text() == "one\n"
        evidence("first-command-and-replay", state=process.state, effects=1)

        pipe = template.model_copy(
            update={
                "operation_id": "pipe-stdin",
                "command": 'read line; printf "reply:%s" "$line"',
                "wait_seconds": 0.0,
            }
        )
        piped = await terminal.submit(pipe)
        await terminal.write_stdin(piped, "hello\n", close=True)
        piped = await done(terminal, piped)
        page = await terminal.read_output(piped, 0)
        assert piped.exit_code == 0 and "reply:hello" in page.text
        evidence("stdin-and-eof", output=page.text)

        service_request = template.model_copy(
            update={
                "operation_id": "persistent-service",
                "command": "printf service-ready; sleep 120",
                "lifecycle": "project_service",
                "timeout_seconds": None,
                "wait_seconds": 0.0,
            }
        )
        service = await terminal.submit(service_request)
        assert service.state == "running"
        await store.close()
        store = await SQLiteStore.open(database)
        repositories = DevelopmentStore(store)
        terminal = compose()
        service = await terminal.inspect(service)
        assert service.state == "running"
        assert (await terminal.submit(service_request)).process_id == service.process_id
        evidence("api-reopen-preserves-service", process_id=service.process_id)

        descendant_request = template.model_copy(
            update={
                "operation_id": "descendant",
                "command": "setsid sh -c 'sleep 120' & printf child-started; wait",
                "wait_seconds": 0.0,
            }
        )
        descendant = await terminal.submit(descendant_request)
        cancelled = await terminal.cancel(descendant)
        assert cancelled.state == "cancelled"
        evidence("container-cancel-descendants", state=cancelled.state)

        await store.request_stop(template.ownership.run_id)
        await terminal.stop_run(template.ownership)
        assert (await terminal.inspect(service)).state == "running"
        with_stop = template.model_copy(update={"operation_id": "after-stop"})
        try:
            await terminal.submit(with_stop)
        except ServiceError as error:
            assert error.code == "cancelled"
        else:
            raise AssertionError("stopped run admitted a process")
        evidence("run-stop-keeps-service-and-rejects-new-command")
        current = await repositories.environments.resolve(environment)
        stopping = replace(
            current,
            revision=current.revision + 1,
            environment=current.environment.model_copy(update={"state": "stopping"}),
        )
        await repositories.environments.compare_and_swap(current, stopping)
        await terminal.stop_environment(environment)
        assert (await repositories.processes.get(service.process_id)).handle.state == "cancelled"
        assert backend is not None
        stopped = await backend.stop(environment)
        assert stopped.state == "stopped"
        evidence("environment-stop-covers-service", state=stopped.state)
    finally:
        cleanup = []
        for container in docker.created:
            inspected = await docker.run(("inspect", container))
            if inspected.code != 0:
                cleanup.append({"id": container, "already_absent": True})
                continue
            value = json.loads(inspected.stdout)[0]
            labels = value["Config"].get("Labels", {})
            if (
                value["Id"] != container
                or value["Image"] != image
                or labels.get("io.krim.development.owner") != template.ownership.owner_id
                or labels.get("io.krim.development.project") != template.environment.project_id
            ):
                cleanup.append({"id": container, "refused_binding_mismatch": True})
                continue
            result = await docker.run(("rm", "--force", container))
            cleanup.append({"id": container, "exit_code": result.code})
        if supervisor.returncode is None:
            supervisor.terminate()
        try:
            await asyncio.wait_for(supervisor.wait(), 5)
        except TimeoutError:
            supervisor.kill()
            await supervisor.wait()
        worker_output.close()
        await store.close()
        evidence(
            "cleanup",
            containers=cleanup,
            supervisor_exit=supervisor.returncode,
            project_retained=str(content),
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--image", required=True)
    args = parser.parse_args()
    asyncio.run(main(args.evidence, args.image))
