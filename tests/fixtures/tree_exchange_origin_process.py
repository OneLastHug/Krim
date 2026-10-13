"""原受控生成封存→同事务来源交换效果后SIGKILL与另进程原SQL核实。"""

import asyncio
import json
import os
import sys
from pathlib import Path

import krim_agent
import krim_agent.workspace_tree_exchange as physical
from krim_agent.config import Settings
from krim_agent.execution import LinuxSandboxRunner
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.completed_tree_seals import CompletedTreeSealStore
from krim_agent.persistence.workspace_tree_exchanges import WorkspaceTreeExchangeStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import PythonAI
from krim_agent.tools.builtins import builtin_registry
from krim_agent.workspace import Workspace
from krim_agent.workspace_tree_exchange import WorkspaceTreeExchangeWriter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_tree_exchange_origins import prepared  # noqa: E402
from test_workspace_tree_exchange_ledger import action, publish  # noqa: E402


async def main():
    phase, folder = sys.argv[1:3]
    root = Path(folder)
    database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
    await database.initialize()
    if phase == "produce":
        runtime = ChatRuntime(
            KrimRepository(database), FakeProvider(delay=0), builtin_registry(), Settings()
        )
        (
            service,
            agent,
            scope,
            trees,
            plan,
            binding,
            writer,
            seals,
            store,
            request,
            change,
        ) = await prepared(runtime, root / "files", persistent=True)
        try:
            proposal, _ = await store.preview(scope, binding, request, change)
            attempt, _ = await store.reserve(
                scope, action(proposal.view), binding.fingerprint, change
            )
            effect = await publish(store, writer, scope, attempt)
            assert effect.receipt.status == "done"
            value = {
                "pid": os.getpid(),
                "package": str(krim_agent.__file__),
                "scope": scope,
                "agent_id": agent.agent_id,
                "proposal_id": proposal.view.proposal_id,
                "operation_id": attempt.view.operation_id,
                "source_plan_id": plan.plan_id,
                "old_owner": store.owner_id,
                "left_inode": effect.observed_left.entries[0].directory.inode,
                "right_inode": effect.observed_right.entries[0].directory.inode,
            }
            (root / "metadata.json").write_text(json.dumps(value))
            print(json.dumps(value), flush=True)
            assert (await asyncio.to_thread(sys.stdin.readline)).strip() == "release"
        finally:
            await service.close()
            await runtime.close()
            await database.close()
    else:
        assert phase == "recover"
        value = json.loads((root / "metadata.json").read_text())
        calls = 0

        def forbidden(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            raise AssertionError(
                "原来源/unknown核实不得重跑Provider/命令/封存/prepare/exchange/apply"
            )

        FakeProvider.stream = forbidden
        LinuxSandboxRunner.start = forbidden
        CompletedTreeSealStore.seal = forbidden
        physical.rename_at = forbidden
        WorkspaceTreeExchangeWriter.prepare = forbidden
        WorkspaceTreeExchangeWriter.apply = forbidden
        ai = await PythonAI.create(
            settings=Settings(database_url=str(database.engine.url)), provider=FakeProvider(delay=0)
        )
        service = ai.agents()
        service.register_workspace("files", root / "files", version="1", writable=True)
        service.register_execution("sandbox", LinuxSandboxRunner(), version="1", owned=True)
        assert await service.restore_persistent() == 1
        store = WorkspaceTreeExchangeStore(service.workspace_edits)
        try:
            assert await store.recover_interrupted() == 1
            operation = await store.operation(value["scope"], value["operation_id"])
            assert operation.view.status == "uncertain" and operation.intent_recorded
            stale = WorkspaceTreeExchangeStore(service.workspace_edits)
            stale.owner_id = value["old_owner"]
            for check in (stale.assert_execution, stale.renew):
                try:
                    await check(value["scope"], value["operation_id"])
                except ServiceError:
                    pass
                else:
                    raise AssertionError("迟到原owner获得交换/续租资格")
            proposal = await store.proposal(value["scope"], value["proposal_id"])
            assert (
                proposal.view.origin == "completed_tree"
                and proposal.view.source_plan_id == value["source_plan_id"]
            )
            with Workspace(root / "files", writable=True) as workspace:
                effect = WorkspaceTreeExchangeWriter(workspace).reconcile(operation.prepared)
                assert effect.receipt.status == "done"
            try:
                await stale.settle(value["scope"], value["operation_id"], effect)
            except ServiceError:
                pass
            else:
                raise AssertionError("迟到原owner保存回执")
            result = await store.settle(
                value["scope"], value["operation_id"], effect, resolving=True
            )
            assert (
                result.view.status == "done" and result.view.operation_id == value["operation_id"]
            )
            assert (
                await store.proposal(value["scope"], value["proposal_id"])
            ).view.origin == "completed_tree"
            # 新进程先恢复原授权作用域，保存同一operation的实际双after；不重复原namespace。
            assert calls == 0 and (root / "files/live/sub/data.bin").read_bytes() == b"\0\xff"
            assert effect.observed_left.entries[0].directory.inode == value["left_inode"]
            assert effect.observed_right.entries[0].directory.inode == value["right_inode"]
            print(
                json.dumps(
                    {
                        "package": str(krim_agent.__file__),
                        "operation_id": operation.view.operation_id,
                        "source_plan_id": proposal.view.source_plan_id,
                        "origin": proposal.view.origin,
                        "status": result.view.status,
                        "replay_calls": calls,
                        "old_owner_fenced": True,
                        "left_inode": effect.observed_left.entries[0].directory.inode,
                        "right_inode": effect.observed_right.entries[0].directory.inode,
                    }
                ),
                flush=True,
            )
        finally:
            await service.close()
            await ai.close()
            await database.close()


asyncio.run(main())
