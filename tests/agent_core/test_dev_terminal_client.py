from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path

import pytest

from krim_agent.dev_terminal.client import WorkerClient
from krim_agent.dev_terminal.worker import Supervisor
from krim_agent.development.contracts import (
    AuthorizationRef,
    CommandRequest,
    EnvironmentHandle,
    ProcessHandle,
    RunOwnership,
)
from krim_agent.development.ports import RuntimeExecution, command_digest


def command(**updates):
    return CommandRequest(
        operation_id="operation",
        environment=EnvironmentHandle(
            owner_id="owner", project_id="project", environment_id="env",
            generation=1, state="ready",
        ),
        ownership=RunOwnership(owner_id="owner", session_id="session", run_id="run"),
        authorization=AuthorizationRef(
            actor_id="actor", actor_generation=1, project_id="project", arguments_digest="0" * 64,
        ),
        command="printf completed", wait_seconds=0.01, timeout_seconds=2,
    ).model_copy(update=updates)


def handle(request):
    return ProcessHandle(
        process_id="process", operation_id=request.operation_id, environment=request.environment,
        ownership=request.ownership, lifecycle=request.lifecycle, state="accepted", log_ref="log",
    )


def execution(request, process):
    return RuntimeExecution(
        execution_id="execution", environment=request.environment,
        operation_id=request.operation_id, process_id=process.process_id,
        attach_argv=("/bin/sh", "-c", request.command), pty=request.pty,
    )


@pytest.mark.asyncio
async def test_trusted_attachment_binds_shared_digest_and_operation(tmp_path: Path):
    project = tmp_path / "project"
    project.mkdir()
    supervisor = Supervisor(tmp_path / "state", project, trusted_attachments=True)

    async def call(payload):
        return {"ok": True, "result": await supervisor.dispatch(payload)}

    client = WorkerClient(call)
    request = command(command="printf '中文'")
    process = handle(request)
    target = execution(request, process)
    try:
        reply = await client.attach(request, process, target)
        assert reply["request_digest"] == command_digest(request)
        await asyncio.wait_for(supervisor.processes["process"].finished.wait(), 3)
        assert (await client.inspect(request, process))["state"] == "exited"
        changed = process.model_copy(update={"process_id": "another-process"})
        with pytest.raises(ValueError, match="operation already bound"):
            await client.attach(request, changed, replace(target, process_id="another-process"))
        observed = request.model_copy(update={
            "environment": request.environment.model_copy(update={"state": "stopping"}),
        })
        assert (await client.inspect(observed, process))["state"] == "exited"
    finally:
        await supervisor.close()


@pytest.mark.asyncio
async def test_unbound_runtime_cannot_launch_and_worker_missing_is_unknown(tmp_path: Path):
    project = tmp_path / "project"
    project.mkdir()
    supervisor = Supervisor(tmp_path / "state", project, trusted_attachments=True)

    async def call(payload):
        return {"ok": True, "result": await supervisor.dispatch(payload)}

    client = WorkerClient(call)
    request = command()
    process = handle(request)
    try:
        target = replace(execution(request, process), operation_id="wrong")
        with pytest.raises(ValueError, match="execution binding"):
            await client.attach(request, process, target)
        assert not supervisor.processes
        assert (await client.inspect(request, process))["state"] == "unknown"
    finally:
        await supervisor.close()
