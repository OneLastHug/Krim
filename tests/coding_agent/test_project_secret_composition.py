"""Current SQLite authorization plus real bootstrap/writer/Secret consumers.

The native fixture substitutes only Docker exec and fixed /tmp paths. It runs
all fixed scripts and the exact admitted command, not Docker/runsc isolation.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import shlex
import signal
import subprocess
import sys
from pathlib import Path

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.dev_environment.bootstrap import CWD_EXEC
from krim_agent.development.contracts import OperationReceipt
from krim_agent.development.ports import RuntimeExecution, command_digest
from krim_agent.development.secret_delivery import CommandSecretLifecycle, CommandSecretRouter
from krim_agent.project_access.process_bootstrap import (
    SECRET_WRITE_CODE,
    secret_token,
    secret_wait_bootstrap,
)
from krim_agent.project_access.secrets import encrypt_secret
from krim_agent.project_access.service import ProjectAccessService
from tests.coding_agent.test_project_secret_authorization import prepare_command
from tests.coding_agent.test_project_secret_bootstrap import _patched_script


@pytest.mark.parametrize("change", ["rotate", "revoke", "expire", "stop", "cancel"])
async def test_real_consumer_current_sqlite_and_lifecycle_cleanup(tmp_path, change):
    code = (
        "import sys,hashlib,time,os; "
        "v=os.read(int(os.environ['KRIM_SECRET_FD_0']),8193); "
        "assert all(v not in x for x in [open('/proc/self/cmdline','rb').read(),"
        "open('/proc/self/environ','rb').read()]); "
        "print(hashlib.sha256(v).hexdigest(),flush=True); time.sleep(30)"
    )
    command = "exec " + shlex.join((sys.executable, "-I", "-c", code))
    fixture = await prepare_command(tmp_path / "access.sqlite", command=command)
    consumers = []
    writers = []
    stops = []

    def start(binding, source):
        assert binding == fixture.binding
        writer = subprocess.Popen(
            (sys.executable, "-I", "-c", _patched_script(SECRET_WRITE_CODE, tmp_path)),
            stdin=source, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env={"PATH": "/usr/bin:/bin"}, start_new_session=True,
        )
        writers.append(writer)
        return writer

    router = CommandSecretRouter(fixture.service._vault, start=start)
    service = ProjectAccessService(fixture.repository, fixture.service._vault,
                                   secret_sink=fixture.sink, git_sink=fixture.sink,
                                   command_sink=router)

    async def workspace(request):
        assert request == fixture.request
        return fixture.workspace_id

    async def stop(binding):
        assert binding == fixture.binding
        stops.append(binding.execution_id)
        for consumer in consumers:
            if consumer.poll() is None:
                try:
                    os.killpg(consumer.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            consumer.wait(timeout=3)

    lifecycle = CommandSecretLifecycle(service, router, workspace=workspace,
                                       stop=stop, interval=.02)
    process = await fixture.development.processes.get(fixture.binding.process_id)
    execution = RuntimeExecution(fixture.binding.execution_id, fixture.request.environment,
                                 fixture.request.operation_id, fixture.binding.process_id,
                                 ("fixed-native-fixture",))
    content = tmp_path / "project-content"
    cwd_exec = CWD_EXEC.replace("'/workspace'", repr(str(content)))
    bootstrap = _patched_script(secret_wait_bootstrap(
        fixture.request, fixture.binding.process_id, cwd_exec, fixture.request.timeout_seconds,
    ), tmp_path).replace("WAIT_SECONDS=0.15", "WAIT_SECONDS=10.0")
    consumer = await asyncio.to_thread(
        subprocess.Popen,
        (sys.executable, "-I", "-c", bootstrap, fixture.request.cwd, fixture.request.command),
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        env={"PATH": "/usr/bin:/bin"}, start_new_session=True,
    )
    consumers.append(consumer)
    try:
        await lifecycle.launched(process, execution)
        digest = await asyncio.wait_for(asyncio.to_thread(consumer.stdout.readline), 3)
        assert digest.strip().decode() == hashlib.sha256(b"fixture-secret").hexdigest()
        assert consumer.poll() is None
        secret_dir = Path(str(tmp_path / "secret-") + secret_token(
            fixture.request, fixture.binding.process_id,
        ))
        assert not await asyncio.to_thread(secret_dir.exists)
        assert b"fixture-secret" not in fixture.database.read_bytes()
        # SDK finishes the launch operation once Shell returns a running handle.
        # This must NOT kill the authorized command or permit another delivery.
        await fixture.development.operations.save_receipt(OperationReceipt(
            identity=fixture.identity, state="completed", result_ref=fixture.identity.call_id,
        ))
        await service.validate_command_secrets(fixture.binding)
        with pytest.raises(ServiceError):
            await service.deliver_command_secrets(fixture.binding)
        await asyncio.sleep(.05)
        assert consumer.poll() is None
        if change == "stop":
            await fixture.store.request_stop(fixture.request.ownership.run_id)
        elif change == "cancel":
            await lifecycle.invalidate(fixture.request.environment.owner_id, fixture.project_id)
        else:
            secret = fixture.secrets["image-api"]
            changes = {"revision": 2}
            if change == "revoke":
                changes["revoked"] = True
            if change == "expire":
                changes["expires_at"] = 0
            replacement = encrypt_secret(fixture.service._vault,
                                         secret.model_copy(update=changes), b"replacement-only")
            await fixture.access_repository.put(replacement, expected_revision=1)
        for _ in range(100):
            if not lifecycle._active:
                break
            await asyncio.sleep(.02)
        assert not lifecycle._active
        assert consumer.poll() is not None
        assert stops == [fixture.binding.execution_id]
        assert not router._slots and not router._queue._entries
        # Existing durable reservation can never authorize Secret re-delivery.
        reservation = await fixture.development.processes.reserve(
            fixture.request, command_digest(fixture.request),
        )
        assert not reservation.execute
        with pytest.raises(ServiceError):
            await service.release_command_secrets(fixture.binding)
        assert len(writers) == 1
    finally:
        await lifecycle.close()
        for consumer in consumers:
            if consumer.poll() is None:
                try:
                    os.killpg(consumer.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            consumer.wait(timeout=3)
            consumer.stdout.close()
        for writer in writers:
            writer.wait(timeout=3)
        await fixture.store.close()
