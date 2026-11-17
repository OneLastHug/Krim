"""Real local FD transport and lifecycle fixtures, not Docker/runsc acceptance."""
from __future__ import annotations

import asyncio
import hashlib
import os
import subprocess
import sys
import time
from dataclasses import replace

import pytest

from krim_agent.account_mcp.secrets import AuthenticatedSecretVault, MasterKeyring
from krim_agent.contracts import ServiceError
from krim_agent.dev_terminal.backend import TerminalBackend
from krim_agent.dev_terminal.client import WorkerClient
from krim_agent.dev_terminal.worker import Supervisor
from krim_agent.development.contracts import (
    AuthorizationRef,
    CommandRequest,
    EnvironmentHandle,
    ProcessHandle,
    RunOwnership,
)
from krim_agent.development.ports import (
    ProcessRecord,
    ProcessReservation,
    RuntimeExecution,
    RuntimeStatus,
    command_digest,
)
from krim_agent.development.secret_delivery import CommandSecretLifecycle, CommandSecretRouter
from krim_agent.project_access.process_secrets import CommandSecretBinding
from krim_agent.project_access.secrets import SecretMaterial
from krim_agent.project_access.service import command_intent
from krim_agent.project_access.types import ResolvedAccess

TOKEN = b"synthetic-D3-local-only-8117"


def fixture_binding():
    environment = EnvironmentHandle(owner_id="owner", project_id="project",
                                    environment_id="env", generation=1, state="ready")
    ref = AuthorizationRef(actor_id="owner", actor_generation=1, project_id="project",
                           arguments_digest="0" * 64, secret_refs=("api",))
    request = CommandRequest(operation_id="operation", environment=environment,
                             ownership=RunOwnership(owner_id="owner", session_id="session",
                                                    run_id="run"),
                             authorization=ref, command="fixture", timeout_seconds=60,
                             wait_seconds=0)
    intent = command_intent(request, workspace_id="workspace")
    request = request.model_copy(update={"authorization": ref.model_copy(
        update={"arguments_digest": intent.digest(ref)},
    )})
    return CommandSecretBinding(request, "workspace", "process", "a" * 64, (("api", 1),))


def process_record(binding):
    request = binding.request
    return ProcessRecord(request, ProcessHandle(
        process_id=binding.process_id, operation_id=request.operation_id,
        environment=request.environment, ownership=request.ownership,
        lifecycle=request.lifecycle, state="accepted", log_ref="log",
    ), command_digest(request), execution_id=binding.execution_id)


class LocalWriter:
    """Trusted fixture consumes the inherited anonymous FD; only emits a hash."""

    def __init__(self):
        self.processes = []
        self.packages = []

    def __call__(self, binding, source):
        # Prove the parent doesn't expose a pathname or bytes in argv/env.
        link = os.readlink(f"/proc/self/fd/{source.fileno()}")
        assert "memfd:" in link or "(deleted)" in link
        code = (
            "import sys,json,base64,hashlib; p=json.load(sys.stdin); "
            "v=base64.b64decode(p['values'][0]); "
            "print(hashlib.sha256(v).hexdigest(),flush=True)"
        )
        child = subprocess.Popen((sys.executable, "-I", "-c", code), stdin=source,
                                 stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                 env={"PATH": "/usr/bin:/bin"}, start_new_session=True)
        self.processes.append(child)
        return child


async def test_router_real_anonymous_stdin_is_encrypted_one_shot_and_cleans():
    binding = fixture_binding()
    vault = AuthenticatedSecretVault(MasterKeyring.create(b"r" * 32))
    writer = LocalWriter()
    router = CommandSecretRouter(vault, start=writer)
    router.register(binding)
    try:
        router.deliver_command(binding, (SecretMaterial(TOKEN),), expires_at=time.time() + 10)
        entry = next(iter(router._queue._entries.values()))
        assert TOKEN.decode() not in repr(entry)
        checks = []
        router.release_command(binding, lambda: checks.append(True))
        await router.wait_writer(binding)
        assert checks == [True]
        child = writer.processes[0]
        assert child.stdout.read().strip().decode() == hashlib.sha256(TOKEN).hexdigest()
        assert TOKEN.decode() not in repr(child.args)
        assert not router._queue._entries
        with pytest.raises(ServiceError):
            router.release_command(binding, lambda: None)
        fresh = CommandSecretRouter(vault, start=writer)
        try:
            # Recovery has no pending data or known fresh reservation.
            with pytest.raises(ServiceError):
                fresh.release_command(binding, lambda: None)
        finally:
            fresh.close()
    finally:
        router.close()
    assert not router._slots


async def test_router_denies_binding_drift_and_failed_spawn_is_consuming():
    binding = fixture_binding()
    calls = []

    def fail(actual, source):
        calls.append(actual)
        raise RuntimeError(TOKEN.decode())

    router = CommandSecretRouter(AuthenticatedSecretVault(MasterKeyring.create(b"r" * 32)),
                                 start=fail)
    router.register(binding)
    try:
        router.deliver_command(binding, (SecretMaterial(TOKEN),), expires_at=time.time() + 10)
        with pytest.raises(ServiceError):
            router.release_command(replace(binding, execution_id="b" * 64), lambda: None)
        with pytest.raises(ServiceError) as error:
            router.release_command(binding, lambda: None)
        assert TOKEN.decode() not in str(error.value)
        assert error.value.__context__ is None
        with pytest.raises(ServiceError):
            router.release_command(binding, lambda: None)
        assert calls == [binding]
    finally:
        router.close()


class AccessFixture:
    """Lifecycle timing fixture; SQLite authorization is tested separately."""

    def __init__(self, router, binding):
        self.router, self.binding = router, binding
        self.allowed = True
        self.receipt = ResolvedAccess(actor_id="owner", actor_generation=1,
                                      project_id="project", operation_id="operation",
                                      arguments_digest=binding.request.authorization.arguments_digest,
                                      grant_revision=1, secret_revisions=(("api", 1),))

    async def resolve(self, ref, intent):
        return self.receipt

    async def deliver_command_secrets(self, binding):
        assert binding == self.binding
        self.router.deliver_command(binding, (SecretMaterial(TOKEN),),
                                    expires_at=time.time() + 10)

    async def release_command_secrets(self, binding):
        self.router.release_command(binding, lambda: None)

    async def validate_command_secrets(self, binding):
        if not self.allowed:
            raise ServiceError("permission_denied", 403)
        return self.receipt


async def test_watcher_stops_exact_unit_and_erases_pending_on_authority_change():
    binding = fixture_binding()
    router = CommandSecretRouter(AuthenticatedSecretVault(MasterKeyring.create(b"r" * 32)),
                                 start=LocalWriter())
    access = AccessFixture(router, binding)
    stopped = asyncio.Event()
    calls = []

    async def workspace(request):
        return "workspace"

    async def stop(actual):
        calls.append(actual)
        stopped.set()

    lifecycle = CommandSecretLifecycle(access, router, workspace=workspace, stop=stop, interval=.02)
    record = process_record(binding)
    execution = RuntimeExecution(binding.execution_id, binding.request.environment,
                                 binding.request.operation_id, binding.process_id, ("fixed",))
    try:
        await lifecycle.launched(record, execution)
        access.allowed = False
        await asyncio.wait_for(stopped.wait(), 2)
        await asyncio.sleep(.02)
        assert calls == [binding]
        assert not router._slots and not router._queue._entries
        assert not lifecycle._active
    finally:
        await lifecycle.close()


async def test_watcher_cleanup_can_finish_in_settled_child_without_join_cycle():
    binding = fixture_binding()
    router = CommandSecretRouter(AuthenticatedSecretVault(MasterKeyring.create(b"r" * 32)),
                                 start=LocalWriter())
    access = AccessFixture(router, binding)
    record = process_record(binding)
    finished = replace(record, handle=record.handle.model_copy(
        update={"state": "cancelled", "exit_code": -9},
    ))
    cleaned = asyncio.Event()

    async def workspace(request):
        return "workspace"

    async def authorize(*args):
        return None

    terminal = TerminalBackend(None, None, None, authorize=authorize)

    async def cleanup():
        await lifecycle.finished(finished)
        cleaned.set()

    async def stop(actual):
        assert actual == binding
        await terminal._settled(cleanup())

    lifecycle = CommandSecretLifecycle(access, router, workspace=workspace, stop=stop, interval=.02)
    execution = RuntimeExecution(binding.execution_id, binding.request.environment,
                                 binding.request.operation_id, binding.process_id, ("fixed",))
    try:
        await lifecycle.launched(record, execution)
        access.allowed = False
        await asyncio.wait_for(cleaned.wait(), 2)
        assert not lifecycle._active and not router._slots
    finally:
        await lifecycle.close()


async def test_stop_failure_still_kills_pending_writer_and_retains_exact_cleanup_binding():
    binding = fixture_binding()
    children = []

    def waiting_writer(actual, source):
        assert actual == binding
        child = subprocess.Popen(("/bin/sleep", "30"), stdin=source,
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        children.append(child)
        return child

    router = CommandSecretRouter(AuthenticatedSecretVault(MasterKeyring.create(b"r" * 32)),
                                 start=waiting_writer)
    access = AccessFixture(router, binding)

    async def workspace(request):
        return "workspace"

    async def stop(actual):
        raise OSError("fixture stop unavailable")

    lifecycle = CommandSecretLifecycle(access, router, workspace=workspace, stop=stop)
    router.register(binding)
    router.deliver_command(binding, (SecretMaterial(TOKEN),), expires_at=time.time() + 10)
    router.release_command(binding, lambda: None)
    lifecycle._active[binding.process_id] = binding
    try:
        with pytest.raises(ServiceError, match="execution_error"):
            await lifecycle.invalidate(binding.request.environment.owner_id,
                                       binding.request.environment.project_id)
        assert children[0].poll() is not None
        assert not router._slots and not router._queue._entries
        assert lifecycle._active[binding.process_id] == binding
        assert not lifecycle.tracks(process_record(binding))

        async def stop_succeeds(actual):
            assert actual == binding

        lifecycle.stop = stop_succeeds
        await lifecycle.close()
        assert not lifecycle._active
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=3)
        router.close()


@pytest.mark.parametrize("action", ["close", "invalidate"])
async def test_cleanup_attempts_every_scope_member_after_first_stop_failure(action):
    first = fixture_binding()
    second_request = first.request.model_copy(update={"operation_id": "operation-two"})
    second = replace(first, request=second_request, process_id="process-two", execution_id="b" * 64)
    router = CommandSecretRouter(AuthenticatedSecretVault(MasterKeyring.create(b"r" * 32)))
    access = AccessFixture(router, first)
    calls = []

    async def workspace(request):
        return "workspace"

    async def stop(actual):
        calls.append(actual.process_id)
        if actual == first:
            raise OSError("fixture first stop failed")

    lifecycle = CommandSecretLifecycle(access, router, workspace=workspace, stop=stop)
    for binding in (first, second):
        router.register(binding)
        router.deliver_command(binding, (SecretMaterial(TOKEN),), expires_at=time.time() + 10)
        lifecycle._active[binding.process_id] = binding
    with pytest.raises(ServiceError, match="execution_error"):
        if action == "close":
            await lifecycle.close()
        else:
            await lifecycle.invalidate(first.request.environment.owner_id,
                                       first.request.environment.project_id)
    assert calls == [first.process_id, second.process_id]
    assert not router._slots and not router._queue._entries
    assert set(lifecycle._active) == {first.process_id}
    assert router._closed == (action == "close")
    lifecycle._active.clear()
    router.close()


@pytest.mark.parametrize("stopped_state", ["exited", "unknown"])
async def test_recovered_secret_execution_without_watcher_is_stopped_never_replayed(stopped_state):
    binding = fixture_binding()
    record = process_record(binding)
    record = replace(record, handle=record.handle.model_copy(update={"state": "running"}))
    stopped = []

    class Repository:
        async def get(self, process_id):
            assert process_id == binding.process_id
            return record

        async def reserve(self, request, digest):
            assert request == binding.request and digest == command_digest(request)
            return ProcessReservation(record, False)

        async def save(self, expected, updated):
            nonlocal record
            assert expected == record
            record = updated
            return record

    class Executions:
        async def cancel(self, execution):
            assert execution.execution_id == binding.execution_id
            assert execution.process_id == binding.process_id
            stopped.append(execution)
            return RuntimeStatus(binding.execution_id, stopped_state,
                                 -9 if stopped_state == "exited" else None)

        async def inspect(self, execution):
            pytest.fail("unmonitored old Secret execution must not continue")

    async def authorize(*args):
        return None

    async def worker(payload):
        assert payload["action"] == "cancel"
        return {"ok": True, "result": {}}

    router = CommandSecretRouter(AuthenticatedSecretVault(MasterKeyring.create(b"r" * 32)))
    lifecycle = CommandSecretLifecycle(AccessFixture(router, binding), router,
                                     workspace=None, stop=None)
    terminal = TerminalBackend(Repository(), Executions(), WorkerClient(worker),
                               authorize=authorize, secrets=lifecycle)
    try:
        assert not lifecycle.tracks(record)
        result = await terminal.submit(binding.request)
        assert result.state == ("cancelled" if stopped_state == "exited" else "unknown")
        assert result.exit_code == (-9 if stopped_state == "exited" else None)
        assert len(stopped) == 1 and not router._slots
    finally:
        await lifecycle.close()


async def test_terminal_missing_secret_hook_denies_before_admission():
    async def authorize(*args):
        pytest.fail("must fail before admission/authorization side effects")

    terminal = TerminalBackend(None, None, None, authorize=authorize)
    with pytest.raises(ServiceError, match="permission_denied"):
        await terminal.submit(fixture_binding().request)


async def test_secret_command_stdout_stderr_and_pty_echo_never_enter_worker_logs(tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    supervisor = Supervisor(tmp_path / "state", project, allow_same_uid_for_tests=True)
    request = {
        "command": "cat; printf fixed-stderr >&2", "cwd": ".", "pty": False,
        "lifecycle": "run", "timeout_seconds": 10,
        "authorization": {"secret_refs": ["api"]},
        "ownership": {"owner_id": "owner", "session_id": "session", "run_id": "run"},
    }
    from krim_agent.dev_terminal.wire import canonical_digest
    digest = canonical_digest(request)
    try:
        await supervisor.dispatch({"action": "submit", "process_id": "secret-process",
                                   "request": request, "request_digest": digest})
        await supervisor.dispatch({"action": "stdin", "process_id": "secret-process",
                                   "request_digest": digest, "data": TOKEN.decode(), "close": True})
        await asyncio.wait_for(supervisor.processes["secret-process"].finished.wait(), 5)
        page = await supervisor.dispatch({"action": "output", "process_id": "secret-process",
                                         "request_digest": digest, "cursor": 0})
        assert page["text"] == ""
        for path in supervisor.root.iterdir():
            if path.is_file():
                assert TOKEN not in path.read_bytes()
    finally:
        await supervisor.close()
