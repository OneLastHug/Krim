"""真实合成Model原Run-call根撤销/keep效果后SIGKILL与公共同operation核实。"""

import asyncio
import json
import os
import sys
from pathlib import Path

import krim_agent
import krim_agent.workspace_tree_writer as physical
from krim_agent.budgets import UsageBudgetConfig
from krim_agent.config import Settings
from krim_agent.execution import LinuxSandboxRunner
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.completed_tree_seals import CompletedTreeSealStore
from krim_agent.persistence.workspace_tree_moves import WorkspaceTreeMoveStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import PythonAI
from krim_agent.tools.builtins import builtin_registry
from krim_agent.workspace_tree_move import (
    TreeMoveModelSource,
    TreeMoveOperationRequest,
    TreeMoveQueryRequest,
)
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_generated_tree_undo_ledger import counts  # noqa: E402
from test_generated_tree_undo_tools import Provider, setup  # noqa: E402


async def main():
    phase, kind, folder = sys.argv[1:4]
    root = Path(folder)
    action = "apply" if kind == "remove_root" else "keep"
    if phase == "produce":
        database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
        await database.initialize()
        provider = Provider()
        runtime = ChatRuntime(KrimRepository(database), provider, builtin_registry(), Settings())
        service, agent, scope, trees, plan, binding, snapshot, request = await setup(
            runtime,
            root / "files",
            persistent=True,
            usage_budget=UsageBudgetConfig(max_total_tokens=100_000),
        )
        provider.request = request
        store = service.manager.tree_moves.store
        original = store.settle

        async def hold(bound_scope, identifier, effect, **kwargs):
            operation = await store.operation(bound_scope, identifier)
            if operation.view.action == action and effect.receipt.status == "done":
                assert operation.model_source is not None and operation.intent_recorded == (
                    action == "apply"
                )
                value = {
                    "package": str(krim_agent.__file__),
                    "pid": os.getpid(),
                    "agent_id": agent.agent_id,
                    "scope": scope,
                    "plan_id": plan.plan_id,
                    "proposal_id": operation.view.proposal_id,
                    "operation_id": identifier,
                    "run_id": operation.model_source.run_id,
                    "call_id": operation.model_source.call_id,
                    "old_owner": store.owner_id,
                    "provider_calls": provider.calls,
                    "root_inode": effect.observed_after.entries[0].directory.inode,
                    "file_inode": effect.observed_after.entries[-1].file.inode,
                    "counts": await counts(store.database),
                    "status_before_receipt": operation.view.status,
                }
                (root / "metadata.json").write_text(json.dumps(value))
                print(json.dumps(value), flush=True)
                assert (await asyncio.to_thread(sys.stdin.readline)).strip() == "release"
            return await original(bound_scope, identifier, effect, **kwargs)

        store.settle = hold
        try:
            await agent.execute("原Model根撤销与保留", idempotency_key="root-process")
        finally:
            await service.close()
            await runtime.close()
            await database.close()
        return
    assert phase == "recover"
    value = json.loads((root / "metadata.json").read_text())
    calls = 0

    def forbidden(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise AssertionError(
            "公共同operation核实不得重放Provider/生成/命令/seal/prepare/apply/namespace"
        )

    FakeProvider.stream = forbidden
    Provider.stream = forbidden
    LinuxSandboxRunner.start = forbidden
    CompletedTreeSealStore.seal = forbidden
    WorkspaceTreeWriter.prepare = forbidden
    WorkspaceTreeWriter.apply = forbidden
    physical.rename_at = forbidden
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=Provider(),
    ) as ai:
        service = ai.agents()

        async def allow(_definition, _context):
            return "allow"

        ai.runtime.tool_policy = allow
        service.register_workspace("files", root / "files", version="1", writable=True)
        service.register_execution("sandbox", LinuxSandboxRunner(), version="1", owned=True)
        service.register_generated_tree_undos()
        assert await service.restore_persistent() == 1, service.restore_errors
        store = service.manager.tree_moves.store
        assert await store.recover_interrupted() == 0
        original = await store.operation(value["scope"], value["operation_id"])
        assert original.view.status == "uncertain" and original.model_source == TreeMoveModelSource(
            value["run_id"], value["call_id"]
        )
        late = WorkspaceTreeMoveStore(service.workspace_edits)
        late.owner_id = value["old_owner"]
        for check in (late.assert_execution, late.renew):
            try:
                await check(value["scope"], value["operation_id"])
            except ServiceError:
                pass
            else:
                raise AssertionError("迟到原Model owner获准重发/续租")
        result = await service.generated_tree_undo_reconcile(
            value["agent_id"],
            TreeMoveOperationRequest(operation_id=value["operation_id"]),
            confirmed=True,
        )
        assert (
            result.source_plan_id == value["plan_id"]
            and result.operation.operation_id == value["operation_id"]
        )
        assert result.operation.status == "done" and result.operation.receipt.changed == (
            kind == "remove_root"
        )
        saved = await store.operation(value["scope"], value["operation_id"])
        assert saved.effect.observed_after.entries[0].directory.inode == value["root_inode"]
        assert saved.effect.observed_after.entries[-1].file.inode == value["file_inode"]
        assert (root / "files/retained/sub/data.bin").read_bytes() == b"\0\xff"
        assert not (root / "files/tree").exists()
        state = await service.generated_tree_undo_query(
            value["agent_id"],
            TreeMoveQueryRequest(kind="proposal", identifier=value["proposal_id"]),
        )
        assert (
            state.proposal.root_state == ("preserved" if kind == "remove_root" else "kept")
            and state.proposal.generation_status == "done"
        )
        assert calls == 0
        print(
            json.dumps(
                {
                    "package": str(krim_agent.__file__),
                    "plan_id": result.source_plan_id,
                    "operation_id": result.operation.operation_id,
                    "status": result.operation.status,
                    "replay_calls": calls,
                    "old_owner_fenced": True,
                    "original_model_source": True,
                    "root_inode": value["root_inode"],
                    "file_inode": value["file_inode"],
                    "counts": await counts(store.database),
                }
            ),
            flush=True,
        )


asyncio.run(main())
