"""Facade cleanup, task-local secret routing, and durable replay fencing."""

import asyncio
from dataclasses import replace

import pytest
from sqlalchemy import update

from krim_agent.contracts import ServiceError
from krim_agent.development.git_operations import GitCredentialRouter
from krim_agent.project_access.git_execution import GitExecutionResult
from krim_agent.project_access.secrets import DeliveryBinding, SecretMaterial
from krim_agent.storage.sandbox_models import sandbox_session_bindings
from tests.coding_agent.test_development_git_operations import fixture as fixture
from tests.coding_agent.test_development_git_operations import request


@pytest.mark.parametrize("cleanup", ["run", "owner", "all"])
async def test_cleanup_cancels_accepted_work_and_blocks_new_dispatch(fixture, monkeypatch, cleanup):
    f = fixture
    req = request("git.diff", object_id="a" * 40, expected_old_id="b" * 40)
    identity = await f.operation(req)
    entered = asyncio.Event()
    stopped = asyncio.Event()

    async def wait(*args, **kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    monkeypatch.setattr(f.facade.runner, "execute", wait)
    task = asyncio.create_task(f.facade.diff(f.actor, identity, req))
    await asyncio.wait_for(entered.wait(), 2)
    if cleanup == "run":
        await f.facade.close_run(f.ownership)
    elif cleanup == "owner":
        await f.facade.revoke_owner(f.actor.principal_id)
    else:
        await f.facade.close()
    assert task.cancelled() and stopped.is_set() and f.facade._active == {}
    assert (await f.repositories.operations.lookup(identity)).state == "running"
    with pytest.raises(ServiceError, match="cancelled"):
        await f.facade.diff(f.actor, await f.operation(req), req)


async def test_other_run_cleanup_does_not_stop_active_run(fixture, monkeypatch):
    f = fixture
    req = request("git.diff", object_id="a" * 40, expected_old_id="b" * 40)
    identity = await f.operation(req)
    entered = asyncio.Event()
    proceed = asyncio.Event()

    async def wait(intent, ref, request, access, **kwargs):
        entered.set()
        await proceed.wait()
        return GitExecutionResult(
            operation_id=identity.operation_id, action="git.diff", changed=False
        )

    monkeypatch.setattr(f.facade.runner, "execute", wait)
    task = asyncio.create_task(f.facade.diff(f.actor, identity, req))
    await asyncio.wait_for(entered.wait(), 2)
    other = f.ownership.model_copy(update={"run_id": "another-run"})
    await f.facade.close_run(other)
    assert not task.done()
    proceed.set()
    assert not (await task).changed


async def test_run_stop_during_work_rejects_final_result(fixture, monkeypatch):
    f = fixture
    req = request("git.diff", object_id="a" * 40, expected_old_id="b" * 40)
    identity = await f.operation(req)

    async def changed(*args, **kwargs):
        await f.store.request_stop(f.ownership.run_id)
        return GitExecutionResult(
            operation_id=identity.operation_id, action="git.diff", changed=False
        )

    monkeypatch.setattr(f.facade.runner, "execute", changed)
    with pytest.raises(ServiceError, match="cancelled"):
        await f.facade.diff(f.actor, identity, req)
    assert not f.facade._active


async def test_stale_principal_session_and_environment_are_rejected(fixture):
    f = fixture
    req = request("git.diff", object_id="a" * 40, expected_old_id="b" * 40)
    identity = await f.operation(req)
    stale = replace(f.actor, generation=f.actor.generation + 1)
    with pytest.raises(ServiceError):
        await f.facade.diff(stale, identity, req)
    changed = identity.model_copy(
        update={"environment": identity.environment.model_copy(update={"generation": 2})}
    )
    with pytest.raises(ServiceError):
        await f.facade.diff(f.actor, changed, req)
    changed = identity.model_copy(
        update={"ownership": identity.ownership.model_copy(update={"session_id": "wrong-session"})}
    )
    with pytest.raises(ServiceError):
        await f.facade.diff(f.actor, changed, req)
    assert (await f.repositories.operations.lookup(identity)).state == "accepted"
    async with f.store.lock, f.store.engine.begin() as conn:
        await conn.execute(
            update(sandbox_session_bindings)
            .where(sandbox_session_bindings.c.session_id == f.ownership.session_id)
            .values(project_id=None, revision=2)
        )
    with pytest.raises(ServiceError):
        await f.facade.diff(f.actor, identity, req)


async def test_reopened_facade_never_dispatches_running_receipt(fixture):
    f = fixture
    req = request("git.diff", object_id="a" * 40, expected_old_id="b" * 40)
    identity = await f.operation(req)
    await f.facade._claim(identity)
    await f.facade.close()
    replacement = f.make_facade()
    try:
        with pytest.raises(ServiceError, match="sandbox_operation_unknown"):
            await replacement.diff(f.actor, identity, req)
    finally:
        await replacement.close()


async def test_credential_router_is_task_local_and_exactly_bound(fixture):
    f = fixture
    req = request("git.diff", object_id="a" * 40, expected_old_id="b" * 40)
    identity = await f.operation(req)
    from krim_agent.development.git_operations import derive_intent

    derived = derive_intent(req, await f.facade.scope(f.actor, identity, req), identity)
    first = DeliveryBinding(derived.intent, derived.authorization, "fixture", 1)
    second = replace(first, revision=2)
    router = GitCredentialRouter()
    values = [[], []]
    ready = [asyncio.Event(), asyncio.Event()]

    class Sink:
        def __init__(self, index):
            self.index = index

        def deliver(self, binding, material):
            values[self.index].append(bytes(material.borrow()))

    async def deliver(index, binding):
        with router.route(binding, Sink(index)):
            ready[index].set()
            await ready[1 - index].wait()
            material = SecretMaterial(f"fixture-{index}".encode())
            try:
                router.deliver(binding, material)
                with pytest.raises(ServiceError):
                    router.deliver(second if binding == first else first, material)
            finally:
                material.erase()

    await asyncio.gather(deliver(0, first), deliver(1, second))
    assert values == [[b"fixture-0"], [b"fixture-1"]]
    material = SecretMaterial(b"fixture")
    try:
        with pytest.raises(ServiceError):
            router.deliver(first, material)
    finally:
        material.erase()


async def test_failed_release_can_retry_from_same_host_task(fixture, monkeypatch):
    f = fixture
    req = request("git.diff", object_id="a" * 40, expected_old_id="b" * 40)
    identity = await f.operation(req)

    class RetryLease:
        attempts = 0

        async def close(self):
            self.attempts += 1
            if self.attempts == 1:
                raise RuntimeError("fixture cleanup failure")

    lease = RetryLease()

    async def execute(*args, **kwargs):
        f.facade._active[identity.operation_id].lease = lease
        return GitExecutionResult(
            operation_id=identity.operation_id, action="git.diff", changed=False
        )

    monkeypatch.setattr(f.facade.runner, "execute", execute)
    with pytest.raises(ServiceError, match="execution_error"):
        await f.facade.diff(f.actor, identity, req)
    assert identity.operation_id in f.facade._active
    await f.facade.close()
    assert lease.attempts == 2 and not f.facade._active
