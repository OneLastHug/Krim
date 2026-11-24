from __future__ import annotations

from types import SimpleNamespace

import pytest

from krim_agent.contracts import ServiceError
from krim_agent.subagents.configuration import SubagentsConfiguration
from krim_agent.subagents.service import SubagentService


class Store:
    healthy = True
    schema_version = 20

    def __init__(self):
        self.events = []

    async def append(self, run_id, kind, data, **kwargs):
        self.events.append(SimpleNamespace(kind=kind, data=data))

    async def snapshot(self, run_id):
        return SimpleNamespace(events=self.events)

    async def work_read(self, callback):
        class Result:
            def __init__(self, row):
                self.row = row

            def mappings(self):
                return self

            def first(self):
                return self.row

        class Connection:
            async def execute(self, statement):
                return Result({"status": "completed", "result": {"receipt": {"cleanup": "clean"}}})

        return await callback(Connection())


async def test_retained_process_slot_releases_only_after_confirmed_cleanup() -> None:
    store = Store()
    service = SubagentService(store, SubagentsConfiguration(max_tool_processes=1))
    slot = service.process_slot("run")
    await slot.__aenter__()
    assert service.processes.active == 1
    await service.remember_process_debt("run", "operation", slot)
    assert service.processes.active == 1
    assert store.events[-1].kind == "process_cleanup_debt"
    assert await service.reconcile_process_debts("run") is True
    assert service.processes.active == 0
    assert store.events[-1].kind == "process_cleanup_reconciled"


async def test_restart_debt_rejects_new_processes_until_reconciled() -> None:
    service = SubagentService(Store(), SubagentsConfiguration())
    service._orphan_process_debts.add(("run", "operation"))
    with pytest.raises(ServiceError) as error:
        async with service.process_slot("next"):
            raise AssertionError("a new process must not start")
    assert error.value.code == "subagent_cleanup_failed"
    assert service.processes.active == 0
