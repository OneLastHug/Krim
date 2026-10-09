import asyncio
import shutil
import sys

import pytest
from sqlalchemy import update

from krim_agent.execution import CommandSpec, LinuxSandboxRunner, SandboxLimits
from krim_agent.execution.records import CommandRequest
from krim_agent.execution.service import DurableCommandHandle, DurableCommandService
from krim_agent.persistence.command_models import CommandRow
from krim_agent.persistence.commands import CommandStore
from krim_agent.protocol.domain import ChatRequest, ServiceError

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实Linux bubblewrap后端",
)


def request(key="one", code="print('ok')"):
    return CommandRequest(
        request_key=key, command=CommandSpec(argv=["/usr/bin/python3", "-I", "-c", code])
    )


@pytest.fixture
async def execution(repository):
    topic = await repository.create_topic("durable command")
    runner = LinuxSandboxRunner(limits=SandboxLimits(wall_seconds=5))
    service = DurableCommandService(CommandStore(repository.database), runner, owned=True)
    try:
        yield service, "topic:" + topic.id
    finally:
        await service.close()


async def output_ready(handle, value=b"ready"):
    async def wait():
        while value not in (await handle.output())[0]:
            state = await handle.view()
            assert not state.terminal, state
            await asyncio.sleep(0.02)

    await asyncio.wait_for(wait(), 10)


async def test_real_execution_frozen_input_original_key_and_cold_read(execution, monkeypatch):
    service, scope = execution
    calls = 0
    original = service.runner.start

    async def count(*args, **kwargs):
        nonlocal calls
        calls += 1
        return await original(*args, **kwargs)

    monkeypatch.setattr(service.runner, "start", count)
    spec = request(
        code="import time;print('ready',flush=True);time.sleep(.4);"
        "open('out','wb').write(open('in','rb').read()+b'\\xff')"
    )
    frozen_request = spec.model_copy(deep=True)
    files = {"in": b"\0input"}
    first = await service.start(scope, spec, files)
    spec.command.argv[-1] = "print('mutated')"
    files["in"] = b"mutated"
    waiter = asyncio.create_task(first.wait())
    await output_ready(first)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    duplicate = await service.start(scope, frozen_request, {"in": b"\0input"})
    assert duplicate.command_id == first.command_id
    result = await duplicate.wait()
    assert result.outcome == "completed" and result.started and result.processes_reaped
    assert (await first.files())["out"].data == b"\0input\xff"
    assert (await first.files("input"))["in"].data == b"\0input"
    reader = DurableCommandHandle(CommandStore(service.store.database), scope, first.command_id)
    assert (await reader.wait()).command_id == first.command_id
    assert await reader.output() == (b"ready\n", b"")
    assert calls == 1


@pytest.mark.parametrize("mode", ["cancel", "close", "timeout"])
async def test_persisted_cancellation_and_close_wait_for_real_execution(execution, mode):
    service, scope = execution
    handle = await service.start(
        scope, request(code="import time;print('ready',flush=True);time.sleep(30)")
    )
    await output_ready(handle)
    if mode == "cancel":
        requested = await handle.cancel()
        assert requested.cancel_requested and not requested.processes_reaped
    elif mode == "close":
        closing = asyncio.create_task(service.close())
        await asyncio.sleep(0.01)
        closing.cancel()
        await asyncio.sleep(0.01)
        closing.cancel()
        try:
            await closing
        except asyncio.CancelledError:
            pass
    result = await asyncio.wait_for(handle.wait(), 10)
    assert result.outcome == ("timed_out" if mode == "timeout" else "cancelled")
    assert result.processes_reaped and not result.artifacts_complete
    assert await handle.output() == (b"ready\n", b"")
    assert not service.runner._active


async def test_expired_producer_cannot_dispatch_after_ready_gate(execution):
    service, scope = execution
    entered, release = asyncio.Event(), asyncio.Event()
    checks = 0

    async def guard():
        nonlocal checks
        checks += 1
        if checks == 2:
            entered.set()
            await release.wait()

    handle = await service.start(scope, request(), authorize=guard)
    try:
        await asyncio.wait_for(entered.wait(), 5)
        async with service.store.database.sessions.begin() as session:
            await session.execute(
                update(CommandRow)
                .where(CommandRow.command_id == handle.command_id)
                .values(lease_expires_at=0)
            )
        view = await handle.view()
        assert view.phase == "interrupted" and not view.dispatched
        release.set()
        await service.close()
        final = await handle.view()
        assert not final.started and not final.dispatched and not final.stdout_bytes
        assert final.processes_reaped and final.interrupted_at
    finally:
        release.set()


async def test_dispatch_intent_is_durable_before_run_and_unknown_is_never_replayed(
    execution, monkeypatch
):
    service, scope = execution
    entered, release = asyncio.Event(), asyncio.Event()
    original = service.store.authorize

    async def dispatch(*args, **kwargs):
        await original(*args, **kwargs)
        if kwargs.get("dispatch"):
            entered.set()
            await release.wait()

    monkeypatch.setattr(service.store, "authorize", dispatch)
    handle = await service.start(scope, request())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        view = await handle.view()
        assert view.phase == "dispatched" and view.dispatched and not view.started
        await handle.cancel()
        await service.close()
        release.set()
        result = await handle.view()
        assert not result.started and not result.stdout_bytes and result.dispatched
        assert result.processes_reaped
        assert (
            await service.store.reserve(scope, request(), service.resource, service.runner.limits)
        ).token is None
    finally:
        release.set()


async def test_lost_save_response_retries_only_identical_receipt(execution, monkeypatch):
    service, scope = execution
    original = service.store.finish
    calls = 0

    async def saved(*args, **kwargs):
        nonlocal calls
        calls += 1
        result = await original(*args, **kwargs)
        if calls == 1:
            raise OSError("response lost after commit")
        return result

    monkeypatch.setattr(service.store, "finish", saved)
    handle = await service.start(scope, request())
    await handle.wait()
    await service.close()
    assert calls == 2
    events = await service.store.events(scope, handle.command_id)
    assert [item["kind"] for item in events].count("finished") == 1
    assert await handle.output() == (b"ok\n", b"")


async def test_failed_persistence_keeps_unknown_instead_of_reexecuting(execution, monkeypatch):
    service, scope = execution

    async def failed(*_args):
        raise OSError("database not accepting final receipt")

    monkeypatch.setattr(service.store, "finish", failed)
    spec = request(
        code="import time;print('ready',flush=True);time.sleep(.3);open('out','w').write('x')"
    )
    handle = await service.start(scope, spec)
    result = await handle.wait()
    assert result.phase == "interrupted" and result.outcome == "uncertain"
    assert result.started and not result.processes_reaped and not result.artifacts_complete
    assert b"ready" in (await handle.output())[0]
    assert (await service.start(scope, spec)).command_id == handle.command_id
    await service.close()
    assert not service.runner._active


async def test_guard_denial_and_reentrant_wait_close_are_safe(execution):
    service, scope = execution

    async def denied():
        with pytest.raises(ServiceError) as close:
            await service.close()
        assert close.value.code == "execution_service_reentrant"
        raise ServiceError("permission_denied", "denied", 403)

    handle = await service.start(scope, request(), authorize=denied)
    result = await handle.wait()
    assert not result.dispatched and not result.started and result.processes_reaped
    assert result.error_code == "permission_denied"
    assert await handle.output() == (b"", b"")


async def test_cancelled_admission_still_has_one_durable_original(execution, monkeypatch):
    service, scope = execution
    entered, release = asyncio.Event(), asyncio.Event()
    original = service.store.reserve

    async def reserve(*args, **kwargs):
        result = await original(*args, **kwargs)
        entered.set()
        await release.wait()
        return result

    monkeypatch.setattr(service.store, "reserve", reserve)
    accepting = asyncio.create_task(service.start(scope, request()))
    await entered.wait()
    accepting.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await accepting
    view = await service.store.by_key(scope, "one")
    assert view is not None
    result = await service.handle(scope, view.command_id).wait()
    assert result.outcome == "completed"
    assert len(await service.store.list_commands(scope)) == 1


async def test_stopped_run_cancels_real_command_and_preserves_reason(execution, repository):
    service, scope = execution
    run, _ = await repository.begin_run(
        scope.removeprefix("topic:"), ChatRequest(text="test", idempotency_key="run")
    )
    handle = await service.start(
        scope,
        request(code="import time;print('ready',flush=True);time.sleep(30)"),
        run_id=run.id,
        call_id="command-call",
    )
    await output_ready(handle)
    await repository.finish(run.id, "aborted", None, [])
    result = await handle.wait()
    assert result.outcome == "cancelled" and result.error_code == "execution_run_closed"
    assert result.started and result.processes_reaped


async def test_close_cancellation_during_admission_still_drains_accepted_work(
    execution, monkeypatch
):
    service, scope = execution
    entered, release = asyncio.Event(), asyncio.Event()
    original = service.store.reserve

    async def reserve(*args, **kwargs):
        result = await original(*args, **kwargs)
        entered.set()
        await release.wait()
        return result

    monkeypatch.setattr(service.store, "reserve", reserve)
    accepting = asyncio.create_task(
        service.start(scope, request(code="import time;time.sleep(30)"))
    )
    await entered.wait()
    closing = asyncio.create_task(service.close())
    await asyncio.sleep(0.01)
    closing.cancel()
    await asyncio.sleep(0.01)
    closing.cancel()
    release.set()
    handle = await accepting
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert (await handle.view()).processes_reaped
    assert not service.runner._active
    with pytest.raises(ServiceError) as closed:
        await service.start(scope, request("another"))
    assert closed.value.code == "execution_service_closed"
