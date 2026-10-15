"""固定两轮、每轮两条独立原模型树链；真实预算/资格与收束保持。"""

import asyncio
import json
import shutil
import sys
import time
from contextvars import ContextVar

import pytest
import test_tree_exchange_tools as cases
from sqlalchemy import select

from krim_agent.application.agents import AgentService
from krim_agent.config import Settings
from krim_agent.execution.linux import LinuxSandboxRunner
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.command_models import CommandRow
from krim_agent.persistence.database import RunRow
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.tools.builtins import builtin_registry

original_chain = (
    cases.test_actual_generated_plan_model_seal_source_apply_keep_undo_original_operations
)

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要原真实Linux namespace/bubblewrap执行",
)


@pytest.mark.parametrize("round_id", [0, 1])
async def test_two_independent_model_tree_chains_keep_original_budgets_and_drain(
    tmp_path, monkeypatch, round_id
):
    services = {}
    executions = {}
    runners = {}
    worker_context = ContextVar("pressure_worker", default=None)
    original_init = AgentService.__init__
    original_start = LinuxSandboxRunner.start

    def observe(self, runtime, *args, **kwargs):
        original_init(self, runtime, *args, **kwargs)
        services.setdefault(id(runtime), []).append(self)

    monkeypatch.setattr(AgentService, "__init__", observe)

    async def observe_execution(self, *args, **kwargs):
        handle = await original_start(self, *args, **kwargs)
        executions.setdefault(id(self), []).append(handle)
        runners[(worker_context.get(), id(self))] = self
        return handle

    monkeypatch.setattr(LinuxSandboxRunner, "start", observe_execution)
    ready = 0
    lock = asyncio.Lock()
    gate = asyncio.Event()

    async def worker(index):
        nonlocal ready
        marker = worker_context.set(index)
        root = tmp_path / f"worker-{index}"
        workspace = root / "files"
        workspace.mkdir(parents=True)
        database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
        await database.initialize()
        repository = KrimRepository(database)
        runtime = ChatRuntime(repository, FakeProvider(delay=0), builtin_registry(), Settings())
        record = {
            "round": round_id,
            "worker": index,
            "database": str(root / "state.sqlite"),
            "completed": False,
            "services_drained": False,
            "runtime_drained": False,
        }
        error = None
        started = time.perf_counter()
        try:
            async with lock:
                ready += 1
                if ready == 2:
                    gate.set()
            await gate.wait()
            started = time.perf_counter()
            await original_chain(runtime, workspace)
            record["original_chain_completed"] = True
            # 原函数断言11次模型调用、真实来源/原操作/内容与计数，再额外读SQL。
            async with database.sessions() as sql:
                commands = list(await sql.scalars(select(CommandRow)))
                runs = list(await sql.scalars(select(RunRow)))
            assert len(commands) == 1 and commands[0].phase == "finished"
            command = commands[0]
            assert command.dispatched and command.started and command.owner_id is None
            assert command.lease_expires_at is None and command.result is not None
            assert command.result["status"] == "completed" and command.result["exit_code"] == 0
            assert type(command.result["descendants_stopped"]) is int
            assert command.result["descendants_stopped"] >= 0
            assert command.result["artifacts_complete"] is True
            assert len(runs) == 1 and runs[0].status == "completed"
            assert (workspace / "tree/sub/data.bin").read_bytes() == b"\x00\xff"
            assert (workspace / "live/old.bin").read_bytes() == b"old\xff\x00"
            record.update(
                {
                    "completed": True,
                    "command_id": command.command_id,
                    "run_id": runs[0].id,
                    "command_result": command.result,
                    "original_provider_calls": runtime.provider.calls,
                }
            )
        except BaseException as caught:
            error = caught
            record["error_type"] = type(caught).__name__
            record["error"] = str(caught)
        finally:
            cleanup_errors = []
            try:
                for service in services.get(id(runtime), []):
                    try:
                        await service.close()
                        assert not service.manager._handles
                        assert not service.manager.commands._operations
                        assert not service.workspace_trees._tasks
                        assert not service.manager.tree_exchanges._operations
                    except BaseException as caught:
                        cleanup_errors.append(caught)
                record["services_drained"] = not cleanup_errors
                try:
                    await runtime.close()
                    assert all(
                        state.task is None or state.task.done() for state in runtime._runs.values()
                    )
                    record["runtime_drained"] = True
                    owned = [runner for (owner, _), runner in runners.items() if owner == index]
                    assert len(owned) == 1
                    results = []
                    for runner in owned:
                        assert not runner._active
                        handles = executions.get(id(runner), [])
                        assert len(handles) == 1
                        for handle in handles:
                            assert handle._task is not None and handle._task.done()
                            result = await handle.wait()
                            assert result.dispatched and result.started
                            assert result.status == "completed" and result.exit_code == 0
                            assert result.artifacts_complete and result.error_code is None
                            results.append(
                                {
                                    "execution_id": result.execution_id,
                                    "status": result.status,
                                    "started": result.started,
                                    "artifacts_complete": result.artifacts_complete,
                                    "descendants_stopped": result.descendants_stopped,
                                }
                            )
                    record["execution_tasks_drained"] = results
                except BaseException as caught:
                    cleanup_errors.append(caught)
            finally:
                record["cleanup_errors"] = [repr(item) for item in cleanup_errors]
                record["seconds"] = time.perf_counter() - started
                (root / "result.json").write_text(
                    json.dumps(record, ensure_ascii=False, indent=2) + "\n"
                )
                await database.close()
                worker_context.reset(marker)
        if error is not None:
            raise error
        if cleanup_errors:
            raise BaseExceptionGroup("压力任务清理失败", cleanup_errors)
        return record

    tasks = [asyncio.create_task(worker(index)) for index in range(2)]
    try:
        async with asyncio.timeout(120):
            values = await asyncio.gather(*tasks, return_exceptions=True)
        failures = [value for value in values if isinstance(value, BaseException)]
        assert not failures, repr(failures)
        assert len(values) == 2 and all(value["completed"] for value in values)
        (tmp_path / "round-result.json").write_text(
            json.dumps(values, ensure_ascii=False, indent=2) + "\n"
        )
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
