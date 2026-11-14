from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest
from test_dev_environment_execution import execution_env as execution_env
from test_dev_environment_execution import make_process, make_request, prepare_and_persist

from krim_agent.dev_environment import EnvironmentError
from krim_agent.dev_environment.model import RuntimeSpec


@pytest.mark.asyncio
async def test_stop_persists_stopping_before_its_first_daemon_side_effect(execution_env):
    _executions, backend, bindings, _processes, runner, record, _request, _process = execution_env
    observed_states: list[str] = []
    original_run = runner.run

    async def run(args: tuple[str, ...]):
        observed_states.append(bindings.record.environment.state)
        return await original_run(args)

    runner.run = run

    result = await backend.stop(record.environment)

    assert result.state == "stopped"
    assert observed_states
    assert observed_states[0] == "stopping"
    assert bindings.saved[0].environment.state == "stopping"


@pytest.mark.asyncio
async def test_stop_cas_failure_has_no_daemon_side_effect(execution_env):
    _executions, backend, bindings, _processes, runner, record, _request, _process = execution_env

    async def compare_and_swap(expected, updated):
        del expected, updated
        raise EnvironmentError("binding_cas_conflict")

    bindings.compare_and_swap = compare_and_swap

    with pytest.raises(EnvironmentError) as raised:
        await backend.stop(record.environment)

    assert raised.value.code == "binding_cas_conflict"
    assert bindings.record == record
    assert bindings.saved == []
    assert runner.calls == []
    assert runner.containers[record.container_id][1] == "running"


@pytest.mark.asyncio
async def test_process_cleanup_cas_failure_keeps_fence_and_retries_without_losing_unit(
    execution_env,
):
    executions, backend, bindings, processes, runner, record, request, process = execution_env
    execution = await prepare_and_persist(
        executions, processes, request, process.handle.process_id
    )
    spec, _state, _exit_code = runner.containers[execution.execution_id]
    runner.containers[execution.execution_id] = (spec, "running", None)

    original_save = processes.save
    fail_once = True

    async def save(expected, updated):
        nonlocal fail_once
        if fail_once:
            fail_once = False
            raise EnvironmentError("process_cas_conflict")
        return await original_save(expected, updated)

    processes.save = save

    with pytest.raises(EnvironmentError) as raised:
        await backend.stop(record.environment)

    assert raised.value.code == "process_cas_conflict"
    assert bindings.record.environment.state == "stopping"
    assert processes.rows[process.handle.process_id].handle.state == "accepted"
    assert runner.containers[execution.execution_id][1] == "exited"
    assert execution.execution_id not in runner.removed

    result = await backend.stop(record.environment)

    assert result.state == "stopped"
    assert bindings.record.environment.state == "stopped"
    assert processes.rows[process.handle.process_id].handle.state == "exited"
    assert execution.execution_id in runner.removed
    assert execution.execution_id not in runner.containers


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["inspect", "start"])
async def test_inspect_and_start_cannot_clear_stopping_fence(execution_env, action: str):
    _executions, backend, bindings, _processes, runner, record, _request, _process = execution_env
    fenced = replace(
        record,
        environment=record.environment.model_copy(update={"state": "stopping"}),
    )
    bindings.record = fenced
    before_calls = len(runner.calls)

    if action == "inspect":
        result = await backend.inspect(fenced.environment)
        assert result.state == "stopping"
    else:
        with pytest.raises(EnvironmentError) as raised:
            await backend.start(fenced.environment)
        assert raised.value.code == "environment_stopping"

    assert bindings.record.environment.state == "stopping"
    if action == "start":
        assert len(runner.calls) == before_calls


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["stopped", "stopping"])
async def test_prepare_rejects_persisted_nonready_state_even_when_daemon_is_running(
    execution_env, state: str,
):
    executions, _backend, bindings, processes, runner, record, request, process = execution_env
    bindings.record = replace(
        record,
        environment=record.environment.model_copy(update={"state": state}),
    )

    with pytest.raises(EnvironmentError) as raised:
        async with executions.prepare(request, process.handle.process_id):
            raise AssertionError("prepare must not yield")

    assert raised.value.code == "environment_not_ready"
    assert runner.containers[record.container_id][1] == "running"
    assert runner.create_attempts == 0
    assert processes.rows[process.handle.process_id].execution_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["accepted", "running", "unknown"])
async def test_prepare_counts_all_active_rows_before_admitting_over_capacity(
    execution_env, state: str,
):
    executions, _backend, bindings, processes, runner, record, request, process = execution_env
    bindings.record = replace(record, max_processes=1)
    extra_request = make_request(record.environment, operation_id=f"operation-{state}")
    extra = make_process(extra_request, process_id=f"process-{state}", state=state)
    processes.set_row(extra)

    with pytest.raises(EnvironmentError) as raised:
        async with executions.prepare(request, process.handle.process_id):
            raise AssertionError("prepare must not yield")

    assert raised.value.code == "execution_capacity"
    assert runner.create_attempts == 0
    assert runner.calls == []


@pytest.mark.asyncio
async def test_prepare_rejects_reservation_missing_from_environment_listing(execution_env):
    executions, _backend, _bindings, processes, runner, _record, request, process = execution_env
    original_list = processes.list_environment

    async def list_without_target(environment):
        rows = await original_list(environment)
        return tuple(row for row in rows if row.handle.process_id != process.handle.process_id)

    processes.list_environment = list_without_target

    with pytest.raises(EnvironmentError) as raised:
        async with executions.prepare(request, process.handle.process_id):
            raise AssertionError("prepare must not yield")

    assert raised.value.code == "execution_reservation_missing"
    assert runner.calls == []


@pytest.mark.asyncio
async def test_recreate_uses_retired_cleanup_listing_and_receipts_before_rm(execution_env):
    executions, backend, bindings, processes, runner, previous, request, process = execution_env
    execution = await prepare_and_persist(
        executions, processes, request, process.handle.process_id
    )
    execution_spec, _state, _exit_code = runner.containers[execution.execution_id]
    runner.containers[execution.execution_id] = (execution_spec, "running", None)

    current = replace(
        previous,
        environment=previous.environment.model_copy(update={
            "generation": previous.environment.generation + 1,
            "state": "creating",
        }),
        container_id=None,
    )
    bindings.record = current
    runner.register(RuntimeSpec(current))

    cleanup_handles = []
    events: list[tuple[str, Any]] = []
    original_save = processes.save
    original_run = runner.run

    async def list_environment(_environment):
        raise AssertionError("retired cleanup must not use ordinary listing")

    async def list_environment_for_cleanup(environment):
        cleanup_handles.append(environment)
        return tuple(
            row for row in processes.rows.values() if row.request.environment == environment
        )

    async def save(expected, updated):
        events.append(("process_save", updated.handle.state))
        return await original_save(expected, updated)

    async def run(args: tuple[str, ...]):
        events.append(("docker", args))
        return await original_run(args)

    processes.list_environment = list_environment
    processes.list_environment_for_cleanup = list_environment_for_cleanup
    processes.save = save
    runner.run = run

    result = await backend.recreate(current.environment, previous=previous)

    assert result.state == "ready"
    assert result.generation == current.environment.generation
    assert cleanup_handles == [previous.environment]
    assert processes.rows[process.handle.process_id].handle.state == "cancelled"
    assert execution.execution_id in runner.removed
    save_index = next(
        index for index, event in enumerate(events)
        if event == ("process_save", "cancelled")
    )
    rm_index = next(
        index
        for index, event in enumerate(events)
        if event[0] == "docker"
        and event[1][:2] == ("rm", "--force")
        and event[1][2] == execution.execution_id
    )
    assert save_index < rm_index


@pytest.mark.asyncio
async def test_stop_cancels_accepted_row_without_execution_unit(execution_env):
    _executions, backend, bindings, processes, runner, record, _request, process = execution_env

    result = await backend.stop(record.environment)

    row = processes.rows[process.handle.process_id]
    assert result.state == "stopped"
    assert row.handle.state == "cancelled"
    assert row.execution_id is None
    assert runner.removed == []
    assert runner.stop_calls == [record.container_id]
    assert all(
        item.handle.state not in {"accepted", "running", "unknown"}
        for item in processes.rows.values()
    )
    assert bindings.record.environment.state == "stopped"


@pytest.mark.asyncio
async def test_missing_running_unit_keeps_stopping_fence_and_unknown_outcome(execution_env):
    executions, backend, bindings, processes, runner, record, request, process = execution_env
    execution = await prepare_and_persist(
        executions, processes, request, process.handle.process_id
    )
    processes.set_row(
        replace(processes.rows[process.handle.process_id],
                handle=process.handle.model_copy(update={"state": "running"}))
    )
    runner.containers.pop(execution.execution_id)

    with pytest.raises(EnvironmentError) as raised:
        await backend.stop(record.environment)

    assert raised.value.code == "execution_cleanup_unknown"
    assert bindings.record.environment.state == "stopping"
    assert processes.rows[process.handle.process_id].handle.state == "running"
    assert runner.removed == []
    assert runner.stop_calls == []
    assert runner.containers[record.container_id][1] == "running"


@pytest.mark.asyncio
async def test_prepare_exception_records_cancelled_before_removing_new_unit(execution_env):
    executions, _backend, _bindings, processes, runner, _record, request, process = execution_env
    events: list[tuple[str, Any]] = []
    original_save = processes.save
    original_run = runner.run

    async def save(expected, updated):
        events.append(("process_save", updated.handle.state))
        return await original_save(expected, updated)

    async def run(args: tuple[str, ...]):
        events.append(("docker", args))
        return await original_run(args)

    processes.save = save
    runner.run = run

    with pytest.raises(EnvironmentError) as raised:
        async with executions.prepare(request, process.handle.process_id):
            pass

    assert raised.value.code == "execution_receipt_missing"
    row = processes.rows[process.handle.process_id]
    assert row.handle.state == "cancelled"
    assert row.execution_id is not None
    rm_index = next(
        index
        for index, event in enumerate(events)
        if event[0] == "docker" and event[1][:2] == ("rm", "--force")
    )
    save_index = next(
        index for index, event in enumerate(events) if event == ("process_save", "cancelled")
    )
    assert save_index < rm_index
    assert row.execution_id in runner.removed
