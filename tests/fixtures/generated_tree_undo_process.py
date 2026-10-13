"""真实原生成树→SQL根撤销效果后SIGKILL；新进程同来源/operation核实。"""

import asyncio
import json
import os
import sys
from pathlib import Path

import krim_agent
import krim_agent.workspace_tree_writer as physical
from krim_agent.config import Settings
from krim_agent.execution import LinuxSandboxRunner
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.completed_tree_seals import CompletedTreeSealStore
from krim_agent.persistence.workspace_tree_moves import WorkspaceTreeMoveStore
from krim_agent.protocol.domain import ChatRequest, ServiceError
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import PythonAI
from krim_agent.tools.builtins import builtin_registry
from krim_agent.workspace_tree_move import TreeMoveModelSource
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_generated_tree_undo_ledger import counts, prepared  # noqa: E402
from test_workspace_tree_move_ledger import action, publish  # noqa: E402


async def main():
    phase, folder = sys.argv[1:3]
    root = Path(folder)
    if phase == "produce":
        database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
        await database.initialize()
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
            req,
            change,
            source,
        ) = await prepared(runtime, root / "files", persistent=True)
        try:
            run, _ = await runtime.repository.begin_run(
                service._handle(agent.agent_id).topic_id,
                ChatRequest(text="原生成根撤销", idempotency_key="root-undo-model"),
                agent_session_id=agent.session_id,
                agent_id=agent.agent_id,
                agent_owner_id=store.owner_id,
            )
            proposal, _ = await store.preview(
                scope,
                binding,
                req,
                change,
                generated_origin=source,
                model_source=TreeMoveModelSource(run.id, "preview"),
            )
            op, _ = await store.reserve(
                scope,
                action(proposal.view),
                binding.fingerprint,
                change,
                model_source=TreeMoveModelSource(run.id, "apply"),
            )
            effect = await publish(store, writer, scope, op)
            assert effect.receipt.status == "done" and not (root / "files/tree").exists()
            assert (root / "files/retained/sub/data.bin").read_bytes() == b"\0\xff"
            value = {
                "pid": os.getpid(),
                "package": str(krim_agent.__file__),
                "scope": scope,
                "agent_id": agent.agent_id,
                "proposal_id": proposal.view.proposal_id,
                "operation_id": op.view.operation_id,
                "plan_id": plan.plan_id,
                "run_id": run.id,
                "old_owner": store.owner_id,
                "root_inode": effect.observed_after.entries[0].directory.inode,
                "file_inode": effect.observed_after.entries[-1].file.inode,
                "fingerprint": change.before.fingerprint,
                "counts": await counts(database),
            }
            (root / "metadata.json").write_text(json.dumps(value))
            print(json.dumps(value), flush=True)
            assert (await asyncio.to_thread(sys.stdin.readline)).strip() == "release"
            await store.settle(scope, op.view.operation_id, effect)
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
        raise AssertionError("原生成根核实不重放Provider/命令/首次seal/prepare/rename/apply")

    FakeProvider.stream = forbidden
    LinuxSandboxRunner.start = forbidden
    CompletedTreeSealStore.seal = forbidden
    WorkspaceTreeWriter.prepare = forbidden
    WorkspaceTreeWriter.apply = forbidden
    physical.rename_at = forbidden
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=FakeProvider(delay=0),
    ) as ai:
        service = ai.agents()
        service.register_workspace("files", root / "files", version="1", writable=True)
        service.register_execution("sandbox", LinuxSandboxRunner(), version="1", owned=True)
        assert await service.restore_persistent() == 1
        store = service.manager.tree_moves.store
        assert await store.recover_interrupted() == 0
        op = await store.operation(value["scope"], value["operation_id"])
        assert op.view.status == "uncertain" and op.intent_recorded
        assert op.model_source == TreeMoveModelSource(value["run_id"], "apply")
        assert op.prepared.before.fingerprint == value["fingerprint"]
        late = WorkspaceTreeMoveStore(service.workspace_edits)
        late.owner_id = value["old_owner"]
        for check in (late.assert_execution, late.renew):
            try:
                await check(value["scope"], value["operation_id"])
            except ServiceError:
                pass
            else:
                raise AssertionError("迟到owner取得生成根撤销资格")
        binding = service._handle(value["agent_id"]).definition.workspace
        writer = WorkspaceTreeWriter(binding.require())
        effect = writer.reconcile(op.prepared)
        assert effect.receipt.status == "done"
        try:
            await late.settle(value["scope"], value["operation_id"], effect)
        except ServiceError:
            pass
        else:
            raise AssertionError("迟到owner保存原根撤销回执")
        done = await store.settle(value["scope"], value["operation_id"], effect, resolving=True)
        assert done.view.status == "done" and done.view.operation_id == value["operation_id"]
        assert effect.observed_after.entries[0].directory.inode == value["root_inode"]
        assert effect.observed_after.entries[-1].file.inode == value["file_inode"]
        assert (
            await store.proposal_by_key(value["scope"], "root-undo-preview")
        ).view.status == "applied"
        assert await counts(store.database) == [1, 1, 1, 0, 2]
        assert (
            await store.seals.get(value["scope"], binding, value["plan_id"])
        ).origin.snapshot.fingerprint == value["fingerprint"]
        assert calls == 0
        print(
            json.dumps(
                {
                    "package": str(krim_agent.__file__),
                    "operation_id": done.view.operation_id,
                    "plan_id": value["plan_id"],
                    "status": done.view.status,
                    "counts": await counts(store.database),
                    "replay_calls": calls,
                    "old_owner_fenced": True,
                    "root_inode": value["root_inode"],
                    "file_inode": value["file_inode"],
                    "original_model_source": True,
                }
            ),
            flush=True,
        )


asyncio.run(main())
