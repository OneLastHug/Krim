"""最终真实限额读回跨原截止/取消时拒绝许可，SQL意图和原键保持。"""

import asyncio
from threading import Event

import pytest
from test_execution_tree_quotas import tree_root as tree_root

from krim_agent.execution import CommandSpec, LinuxSandboxRunner, SandboxLimits
from krim_agent.execution.cgroups import CgroupLease
from krim_agent.execution.records import CommandRequest
from krim_agent.execution.service import DurableCommandService
from krim_agent.execution.tree_models import SandboxTreeLimits
from krim_agent.persistence.commands import CommandStore


def limits(startup):
    kwargs = {} if startup is None else {"startup_seconds": startup}
    return SandboxLimits(
        **kwargs,
        tree=SandboxTreeLimits(cpu_seconds=3, memory_bytes=134217728, process_count=16),
    )


def final_read_barrier(monkeypatch):
    entered, release = Event(), Event()
    original = CgroupLease.verify_limits
    reads = 0

    def observe(self):
        nonlocal reads
        original(self)
        reads += 1
        if reads == 3:
            entered.set()
            assert release.wait(20), "测试收束门未释放"

    monkeypatch.setattr(CgroupLease, "verify_limits", observe)
    return entered, release


def command():
    return CommandSpec(argv=["/usr/bin/python3", "-I", "-c", "print('BODY_STARTED',flush=True)"])


@pytest.mark.parametrize(
    ("mode", "startup"),
    [("deadline", 1), ("deadline", None), ("cancel", 1)],
    ids=["configured-one", "default-ten", "cancel"],
)
async def test_final_kernel_read_cannot_reset_expired_startup_or_dispatch_after_cancel(
    tree_root, monkeypatch, mode, startup
):
    entered, release = final_read_barrier(monkeypatch)
    selected = limits(startup)
    if startup is None:
        assert selected.startup_seconds == 10
    async with LinuxSandboxRunner(limits=selected, cgroup_root=tree_root) as runner:
        try:
            handle = await runner.start(command())
            assert await asyncio.to_thread(entered.wait, selected.startup_seconds + 3)
            if mode == "deadline":
                await asyncio.sleep(selected.startup_seconds + 0.1)
            else:
                handle.cancel()
            release.set()
            result = await handle.wait()
            assert result.status == ("timed_out" if mode == "deadline" else "cancelled")
            assert not result.dispatched and not result.started
            assert result.stdout == result.stderr == b"" and not result.artifacts_complete
            assert result.error_code == (
                "execution_timeout" if mode == "deadline" else "execution_cancelled"
            )
            assert handle.done and result.tree_usage.membership_verified
            assert result.tree_usage.populated is False and result.tree_usage.leader_reaped
        finally:
            release.set()
    assert not runner._active


@pytest.mark.parametrize("mode", ["deadline", "cancel"])
async def test_late_ready_refusal_keeps_original_sql_dispatch_intent_and_key_without_replay(
    repository, tree_root, monkeypatch, mode
):
    topic = await repository.create_topic("ready gate")
    scope = "topic:" + topic.id
    entered, release = final_read_barrier(monkeypatch)
    runner = LinuxSandboxRunner(limits=limits(1), cgroup_root=tree_root)
    service = DurableCommandService(CommandStore(repository.database), runner, owned=True)
    original = runner.start
    executions = []

    async def observe(*args, **kwargs):
        handle = await original(*args, **kwargs)
        executions.append(handle)
        return handle

    monkeypatch.setattr(runner, "start", observe)
    request = CommandRequest(request_key="original-" + mode, command=command())
    stopping = None
    try:
        handle = await service.start(scope, request)
        assert await asyncio.to_thread(entered.wait, 4)
        intent = await handle.view()
        assert intent.dispatched and not intent.started and not intent.terminal
        if mode == "deadline":
            await asyncio.sleep(1.1)
        else:
            stopping = asyncio.create_task(service.stop_and_wait(scope, handle.command_id))
            assert await asyncio.to_thread(executions[0]._cancel.wait, 3)
            assert not stopping.done()
        release.set()
        result = await handle.wait()
        if stopping is not None:
            assert (await stopping).command_id == handle.command_id
        assert result.outcome == ("timed_out" if mode == "deadline" else "cancelled")
        # SQL派发意图已提交；低层拒绝不能把原意图改成“从未派发”。
        assert result.dispatched and not result.started and result.processes_reaped
        assert not result.artifacts_complete and await handle.output() == (b"", b"")
        duplicate = await service.start(scope, request)
        assert duplicate.command_id == handle.command_id
        assert await duplicate.wait() == result and len(executions) == 1
        actual = await executions[0].wait()
        assert actual.tree_usage.populated is False and actual.tree_usage.leader_reaped
    finally:
        release.set()
        if stopping is not None:
            await asyncio.gather(stopping, return_exceptions=True)
        await service.close()
    assert not runner._active and not service._active
