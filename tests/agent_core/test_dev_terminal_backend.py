"""Real host processes with test-only repository/runtime ports, not Docker proof."""
from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path

import pytest
import pytest_asyncio
from test_dev_terminal_client import command

from krim_agent.contracts import ServiceError
from krim_agent.dev_terminal.backend import TerminalBackend
from krim_agent.dev_terminal.client import WorkerClient, WorkerError
from krim_agent.dev_terminal.worker import Supervisor
from krim_agent.development.contracts import ProcessHandle
from krim_agent.development.ports import (
    ProcessRecord,
    ProcessReservation,
    RuntimeExecution,
    RuntimeStatus,
    command_digest,
)


class Repository:
    """Port double only; I owns and tests the production SQLite repository."""

    def __init__(self):
        self.records = {}
        self.lock = asyncio.Lock()
        self.reservations = 0
        self.conflict_once = False
        self.max_processes = 4
        self.stopping = False

    async def reserve(self, request, digest):
        async with self.lock:
            for record in self.records.values():
                if record.request.operation_id == request.operation_id:
                    if record.request_digest != digest:
                        raise ValueError("operation binding changed")
                    return ProcessReservation(record, False)
            if self.stopping:
                raise ValueError("environment is stopping")
            if sum(row.handle.state in {"accepted", "running", "unknown"}
                   for row in self.records.values()) >= self.max_processes:
                raise ServiceError("capacity", 429)
            self.reservations += 1
            process_id = f"process-{self.reservations}"
            process = ProcessHandle(
                process_id=process_id, operation_id=request.operation_id,
                environment=request.environment, ownership=request.ownership,
                lifecycle=request.lifecycle, state="accepted", log_ref=f"log-{self.reservations}",
            )
            record = ProcessRecord(request=request, handle=process, request_digest=digest)
            self.records[process_id] = record
            return ProcessReservation(record, True)

    async def get(self, process_id):
        return self.records[process_id]

    async def save(self, expected, updated):
        async with self.lock:
            if updated.revision != expected.revision + 1:
                raise ServiceError("input_revision_conflict")
            current = self.records[expected.handle.process_id]
            if self.conflict_once:
                self.conflict_once = False
                self.records[current.handle.process_id] = replace(
                    current, revision=current.revision + 1,
                    handle=current.handle.model_copy(
                        update={"state": "cancelled", "exit_code": -9},
                    ),
                )
                raise ServiceError("revision_conflict")
            if expected != current:
                raise ServiceError("revision_conflict")
            if (current.handle.state == "unknown"
                    and updated.handle.state in {"accepted", "running"}):
                raise ValueError("unknown cannot become active again")
            self.records[updated.handle.process_id] = updated
            return updated

    async def list_environment(self, environment):
        binding = environment.model_dump(exclude={"state"})
        return tuple(record for record in self.records.values()
                     if record.handle.environment.model_dump(exclude={"state"}) == binding)

    async def list_run(self, ownership):
        return tuple(record for record in self.records.values()
                     if record.handle.ownership == ownership)


class LocalRuntime:
    """B test double: attachment is a real local shell, no container isolation."""

    def __init__(self, worker, project):
        self.worker = worker
        self.project = project
        self.targets = {}
        self.prepared = 0
        self.cancel_calls = []
        self.allow_cancel = True
        self.prepare_gate = None
        self.prepare_entered = asyncio.Event()
        self.watchdogs = []
        self.preparing = False
        self.bad_identity = False

    @asynccontextmanager
    async def prepare(self, request, process_id):
        self.prepared += 1
        self.preparing = True
        self.prepare_entered.set()
        if self.prepare_gate is not None:
            await self.prepare_gate.wait()
        target = RuntimeExecution(
            execution_id=f"execution-{self.prepared}", environment=request.environment,
            operation_id=request.operation_id, process_id=process_id,
            attach_argv=("/bin/sh", "-c", request.command), pty=request.pty,
        )
        self.targets[target.execution_id] = target
        try:
            yield replace(target, process_id="wrong") if self.bad_identity else target
            if request.timeout_seconds is not None:
                self.watchdogs.append(asyncio.create_task(
                    self.deadline(target, request.timeout_seconds),
                ))
        finally:
            self.preparing = False

    async def deadline(self, execution, seconds):
        process = self.worker.processes.get(execution.process_id)
        if process is None:
            return
        try:
            await asyncio.wait_for(process.finished.wait(), seconds)
        except TimeoutError:
            await self.cancel(execution)

    async def inspect(self, execution):
        assert not self.preparing, "B.inspect cannot re-enter prepare's project lock"
        target = self.targets.get(execution.execution_id)
        if target is None:
            return RuntimeStatus(execution.execution_id, "unknown")
        assert target.process_id == execution.process_id
        assert target.operation_id == execution.operation_id
        process = self.worker.processes.get(execution.process_id)
        if process is None:
            return RuntimeStatus(execution.execution_id, "created")
        state = process.record["state"]
        if state in {"exited", "cancelled", "failed"}:
            return RuntimeStatus(execution.execution_id, "exited", process.record["exit_code"])
        return RuntimeStatus(execution.execution_id, "running" if state == "running" else "unknown")

    async def cancel(self, execution):
        self.cancel_calls.append(execution.execution_id)
        if not self.allow_cancel:
            return RuntimeStatus(execution.execution_id, "unknown")
        process = self.worker.processes.get(execution.process_id)
        if process is None:
            return RuntimeStatus(execution.execution_id, "exited")
        await process.cancel()
        return await self.inspect(execution)

    async def release(self, execution):
        self.targets.pop(execution.execution_id, None)


@pytest_asyncio.fixture
async def stack(tmp_path: Path):
    project = tmp_path / "project"
    project.mkdir()
    worker = Supervisor(tmp_path / "state", project, trusted_attachments=True)
    repository = Repository()
    runtime = LocalRuntime(worker, project)
    authorizations = []

    async def authorize(request, action):
        authorizations.append((request.operation_id, action))

    async def call(payload):
        if payload["action"] == "submit":
            record = await repository.get(payload["process_id"])
            assert record.execution_id == payload["execution_id"]
            assert runtime.preparing
        return {"ok": True, "result": await worker.dispatch(payload)}

    backend = TerminalBackend(repository, runtime, WorkerClient(call), authorize=authorize)
    try:
        yield backend, repository, runtime, worker, authorizations
    finally:
        await worker.close()
        for task in runtime.watchdogs:
            task.cancel()
        await asyncio.gather(*runtime.watchdogs, return_exceptions=True)


@pytest.mark.asyncio
async def test_wait_budget_and_runtime_deadline_are_independent(stack):
    backend, _, _, worker, _ = stack
    request = command(command="sleep 30", wait_seconds=0.01, timeout_seconds=0.3)
    started = time.monotonic()
    process = await backend.submit(request)
    assert process.state == "running" and time.monotonic() - started < 0.3
    await asyncio.wait_for(worker.processes[process.process_id].finished.wait(), 3)
    result = await backend.inspect(process)
    assert result.state == "exited" and result.exit_code != 0


@pytest.mark.asyncio
async def test_duplicate_operation_and_full_binding_drift(stack):
    backend, repository, runtime, worker, _ = stack
    request = command(command="printf once", wait_seconds=0)
    replies = await asyncio.gather(*(backend.submit(request) for _ in range(6)))
    assert len({reply.process_id for reply in replies}) == 1
    assert repository.reservations == runtime.prepared == 1
    process = replies[0]
    await asyncio.wait_for(worker.processes[process.process_id].finished.wait(), 3)
    result = await backend.read_output(process, 0)
    assert result.text == "once"
    for drift in (
        {"command": "echo different"}, {"cwd": "different"},
        {"ownership": request.ownership.model_copy(update={"run_id": "other"})},
        {"environment": request.environment.model_copy(update={"generation": 2})},
        {"authorization": request.authorization.model_copy(update={"actor_generation": 2})},
    ):
        with pytest.raises(ValueError, match="binding"):
            await backend.submit(request.model_copy(update=drift))
    assert runtime.prepared == 1


@pytest.mark.asyncio
async def test_new_backend_recovers_without_prepare_or_attach(stack):
    backend, repository, runtime, _, authorizations = stack
    request = command(command="sleep 30", wait_seconds=0)
    process = await backend.submit(request)
    recovered = TerminalBackend(repository, runtime, backend.worker, authorize=backend.authorize)
    assert (await recovered.submit(request)).state == "running"
    assert runtime.prepared == 1
    await recovered.write_stdin(process, "")
    assert (request.operation_id, "write_stdin") in authorizations
    await recovered.cancel(process)


@pytest.mark.asyncio
async def test_admitted_without_execution_and_created_recover_unknown_without_replay(stack):
    backend, repository, runtime, worker, _ = stack
    request = command(wait_seconds=0)
    reservation = await repository.reserve(request, command_digest(request))
    assert (await backend.submit(request)).state == "unknown"
    assert runtime.prepared == 0 and not worker.processes
    current = await repository.get(reservation.record.handle.process_id)
    target = RuntimeExecution(
        execution_id="created", environment=request.environment,
        operation_id=request.operation_id, process_id=current.handle.process_id,
        attach_argv=("/bin/sh", "-c", "exit 0"),
    )
    runtime.targets[target.execution_id] = target
    await repository.save(current, replace(
        current, execution_id=target.execution_id, revision=current.revision + 1,
    ))
    assert (await backend.submit(request)).state == "unknown"
    assert runtime.prepared == 0 and not worker.processes
    assert (await backend.cancel(current.handle)).state == "cancelled"


@pytest.mark.asyncio
async def test_cancel_requires_runtime_proof_not_attachment_exit(stack):
    backend, _, runtime, worker, _ = stack
    process = await backend.submit(command(command="sleep 30", wait_seconds=0))
    runtime.allow_cancel = False
    result = await backend.cancel(process)
    assert result.state == "unknown"
    assert worker.processes[process.process_id].record["state"] == "running"
    assert runtime.cancel_calls
    runtime.allow_cancel = True
    assert (await backend.cancel(result)).state == "cancelled"


@pytest.mark.asyncio
async def test_run_stop_keeps_service_environment_stop_covers_no_active_run(stack):
    backend, _, runtime, worker, _ = stack
    request = command(command="sleep 30", wait_seconds=0)
    run = await backend.submit(request)
    service = await backend.submit(command(
        command="sleep 30", operation_id="service", lifecycle="project_service",
        timeout_seconds=None, wait_seconds=0,
    ))
    await backend.stop_run(request.ownership)
    assert (await backend.inspect(run)).state == "cancelled"
    assert (await backend.inspect(service)).state == "running"
    await backend.stop_environment(service.environment)
    assert (await backend.inspect(service)).state == "cancelled"
    assert worker.processes[service.process_id].finished.is_set()
    assert len(runtime.cancel_calls) == 2


@pytest.mark.asyncio
async def test_cancelled_submit_wait_settles_durable_launch_without_replaying(stack):
    backend, repository, runtime, _, _ = stack
    runtime.prepare_gate = asyncio.Event()
    request = command(command="sleep 30", lifecycle="project_service",
                      timeout_seconds=None, wait_seconds=1)
    task = asyncio.create_task(backend.submit(request))
    await runtime.prepare_entered.wait()
    task.cancel()
    runtime.prepare_gate.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    record = next(iter(repository.records.values()))
    assert record.execution_id is not None
    assert (await backend.inspect(record.handle)).state == "running"
    assert runtime.prepared == 1
    await backend.cancel(record.handle)


@pytest.mark.asyncio
async def test_stale_inspect_cannot_overwrite_cancelled_record(stack):
    backend, repository, _, _, _ = stack
    process = await backend.submit(command(command="sleep 30", wait_seconds=0))
    record = await repository.get(process.process_id)
    repository.conflict_once = True
    result = await backend._save_state(record, "unknown", None)
    assert result.handle.state == "cancelled"


@pytest.mark.asyncio
async def test_mutated_handle_and_permission_revocation_rejected(stack):
    backend, _, runtime, _, _ = stack
    process = await backend.submit(command(command="sleep 30", wait_seconds=0))
    forged = process.model_copy(update={"log_ref": "another-log"})
    with pytest.raises(ValueError, match="binding"):
        await backend.read_output(forged, 0)

    async def deny(request, action):
        raise PermissionError("revoked")

    denied = TerminalBackend(backend.repository, runtime, backend.worker, authorize=deny)
    for call in (
        denied.inspect(process), denied.read_output(process, 0),
        denied.write_stdin(process, "bad"), denied.cancel(process),
    ):
        with pytest.raises(PermissionError, match="revoked"):
            await call
    await backend.cancel(process)


@pytest.mark.asyncio
async def test_environment_stop_reports_unknown_cleanup(stack):
    backend, _, runtime, _, _ = stack
    process = await backend.submit(command(command="sleep 30", wait_seconds=0))
    runtime.allow_cancel = False
    with pytest.raises(WorkerError, match="cleanup_unknown"):
        await backend.stop_environment(process.environment)
    runtime.allow_cancel = True
    await backend.stop_environment(process.environment)


@pytest.mark.asyncio
async def test_save_conflict_retires_prepared_unit_before_attach(stack):
    backend, repository, runtime, worker, _ = stack
    repository.conflict_once = True
    process = await backend.submit(command(wait_seconds=0))
    assert process.state == "cancelled"
    assert runtime.cancel_calls == ["execution-1"]
    assert not worker.processes


@pytest.mark.asyncio
async def test_stop_waits_for_launch_handshake_then_cancels(stack):
    backend, _, runtime, worker, _ = stack
    runtime.prepare_gate = asyncio.Event()
    request = command(command="sleep 30", wait_seconds=0)
    task = asyncio.create_task(backend.submit(request))
    await runtime.prepare_entered.wait()
    stop = asyncio.create_task(backend.stop_environment(request.environment))
    await asyncio.sleep(0)
    assert not stop.done()
    runtime.prepare_gate.set()
    process = await task
    await stop
    assert worker.processes[process.process_id].finished.is_set()
    assert (await backend.inspect(process)).state == "cancelled"


@pytest.mark.asyncio
async def test_pty_and_pipe_input_through_persisted_backend(stack):
    backend, _, _, worker, _ = stack
    for tty in (False, True):
        request = command(command="printf ready; cat; printf done", operation_id=f"tty-{tty}",
                          pty=tty, wait_seconds=0)
        process = await backend.submit(request)
        for _ in range(100):
            page = await backend.read_output(process, 0)
            if "ready" in page.text:
                break
            await asyncio.sleep(0.01)
        await backend.write_stdin(process, "hello\n", close=True)
        await asyncio.wait_for(worker.processes[process.process_id].finished.wait(), 5)
        page = await backend.read_output(process, 0)
        assert "hello" in page.text and "done" in page.text and page.eof
        assert (await backend.inspect(process)).exit_code == 0


@pytest.mark.asyncio
async def test_binding_error_from_repository_save_is_not_swallowed(stack):
    backend, repository, _, _, _ = stack
    process = await backend.submit(command(command="sleep 30", wait_seconds=0))
    record = await repository.get(process.process_id)

    async def reject(expected, updated):
        raise ValueError("stored binding changed")

    original = repository.save
    repository.save = reject
    try:
        with pytest.raises(ValueError, match="stored binding"):
            await backend._save_state(record, "unknown", None)
    finally:
        repository.save = original
    await backend.cancel(process)


@pytest.mark.asyncio
async def test_bad_runtime_identity_is_rejected_before_attach(stack):
    backend, _, runtime, worker, _ = stack
    runtime.bad_identity = True
    with pytest.raises(ValueError, match="launch binding"):
        await backend.submit(command(wait_seconds=0))
    assert not worker.processes


@pytest.mark.asyncio
async def test_confirmed_cancel_retries_over_concurrent_nonterminal_cas(stack):
    backend, repository, _, _, _ = stack
    process = await backend.submit(command(command="sleep 30", wait_seconds=0))
    original = repository.save
    conflict = True

    async def race(expected, updated):
        nonlocal conflict
        if conflict and updated.handle.state == "cancelled":
            conflict = False
            repository.records[process.process_id] = replace(
                expected, revision=expected.revision + 1,
                handle=expected.handle.model_copy(update={"state": "unknown"}),
            )
            raise ServiceError("revision_conflict")
        return await original(expected, updated)

    repository.save = race
    cancelled = await backend.cancel(process)
    assert cancelled.state == "cancelled"
    assert (await repository.get(process.process_id)).handle.state == "cancelled"


@pytest.mark.asyncio
async def test_unknown_is_monotonic_until_definitive_terminal_result(stack):
    backend, repository, runtime, _, _ = stack
    request = command(command="sleep 30", wait_seconds=0)
    process = await backend.submit(request)
    current = await repository.get(process.process_id)
    await repository.save(current, replace(
        current, handle=current.handle.model_copy(update={"state": "unknown"}),
        revision=current.revision + 1,
    ))
    assert (await backend.inspect(process)).state == "unknown"
    assert (await backend.submit(request)).state == "unknown"
    assert runtime.prepared == 1
    with pytest.raises(ValueError, match="not running"):
        await backend.write_stdin(process, "unsafe")
    assert (await backend.cancel(process)).state == "cancelled"


@pytest.mark.asyncio
async def test_capacity_429_propagates_unknown_counts_and_stop_rejects_new(stack):
    backend, repository, runtime, _, _ = stack
    repository.max_processes = 1
    request = command(wait_seconds=0)
    admitted = await repository.reserve(request, command_digest(request))
    current = admitted.record
    await repository.save(current, replace(
        current, handle=current.handle.model_copy(update={"state": "unknown"}),
        revision=current.revision + 1,
    ))
    with pytest.raises(ServiceError) as captured:
        await backend.submit(command(operation_id="over-capacity", wait_seconds=0))
    assert captured.value.code == "capacity" and captured.value.status == 429
    assert runtime.prepared == 0
    repository.stopping = True
    with pytest.raises(ValueError, match="stopping"):
        await backend.submit(command(operation_id="after-stop", wait_seconds=0))
    assert repository.reservations == 1 and runtime.prepared == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("revision_delta", [0, -1, 2])
async def test_repository_rejects_missing_or_incorrect_revision_increment(revision_delta):
    repository = Repository()
    request = command(wait_seconds=0)
    current = (await repository.reserve(request, command_digest(request))).record
    updated = replace(current, execution_id="created-unit",
                      revision=current.revision + revision_delta)
    with pytest.raises(ServiceError) as captured:
        await repository.save(current, updated)
    assert captured.value.code == "input_revision_conflict"
    assert await repository.get(current.handle.process_id) == current


@pytest.mark.asyncio
async def test_backend_supplies_each_revision_and_unchanged_inspect_does_not_save(stack):
    backend, repository, _, _, _ = stack
    process = await backend.submit(command(command="sleep 30", wait_seconds=0))
    current = await repository.get(process.process_id)
    # Admission revision 1, execution binding revision 2, running revision 3.
    assert current.execution_id is not None and current.revision == 3
    assert current.handle.state == "running"
    assert (await backend.inspect(process)).state == "running"
    assert await repository.get(process.process_id) == current
    cancelled = await backend.cancel(process)
    saved = await repository.get(process.process_id)
    assert cancelled.state == "cancelled" and saved.revision == 4
    assert (await backend.inspect(cancelled)).state == "cancelled"
    assert await repository.get(process.process_id) == saved
