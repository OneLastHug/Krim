"""实际生成来源/原Model apply或keep效果后SIGKILL；公共核实原SQL零重放。"""

import asyncio
import json
import os
import sys
from pathlib import Path

import krim_agent
import krim_agent.workspace_tree_exchange as physical
from krim_agent.config import Settings
from krim_agent.execution import LinuxSandboxRunner
from krim_agent.persistence.completed_tree_seals import CompletedTreeSealStore
from krim_agent.persistence.workspace_tree_exchanges import WorkspaceTreeExchangeStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import PythonAI
from krim_agent.tools.tree_exchanges import tree_exchange_tools
from krim_agent.workspace_tree_exchange import WorkspaceTreeExchangeWriter
from krim_agent.workspace_tree_exchange_types import (
    TreeExchangeModelSource,
    TreeExchangeOperationRequest,
    TreeExchangeQueryRequest,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_completed_tree_origins import complete  # noqa: E402
from test_tree_exchange_tools import NAMES, Provider, allow, counts  # noqa: E402


async def main():
    phase, kind, folder = sys.argv[1:4]
    root, provider = Path(folder), Provider()
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=provider,
    ) as ai:
        ai.runtime.tool_policy = allow
        if phase == "produce":
            ai.runtime.tools.register_many(*tree_exchange_tools())
            service, agent, scope, trees, plan, binding, snapshot, outputs = await complete(
                ai.runtime, root / "files", persistent=True, extra_names=NAMES
            )
            service.register_tree_exchanges()
            provider.plan_id, provider.left_path, provider.right_path = plan.plan_id, "live", "tree"
            (root / "files/live").mkdir()
            (root / "files/live/old.bin").write_bytes(b"old\xff\0")
            store = service.manager.tree_exchanges.store
            original_settle = store.settle

            async def blocked(bound_scope, identifier, effect, **kwargs):
                operation = await store.operation(bound_scope, identifier)
                if operation.view.action != kind:
                    return await original_settle(bound_scope, identifier, effect, **kwargs)
                assert effect.receipt.status == "done"
                source = (await store.proposal(bound_scope, operation.view.proposal_id)).view
                assert source.origin == "completed_tree" and source.source_plan_id == plan.plan_id
                value = {
                    "pid": os.getpid(),
                    "package": str(krim_agent.__file__),
                    "agent_id": agent.agent_id,
                    "scope": bound_scope,
                    "operation_id": identifier,
                    "proposal_id": operation.view.proposal_id,
                    "run_id": operation.model_source.run_id,
                    "call_id": operation.model_source.call_id,
                    "old_owner": store.owner_id,
                    "source_plan_id": plan.plan_id,
                    "proof_hash": (
                        await store.seals.get(bound_scope, binding, plan.plan_id)
                    ).origin.view.proof_hash,
                    "source_operations": list(
                        snapshot_entry["operation_id"] for snapshot_entry in outputs
                    ),
                    "counts": await counts(store.database),
                    "provider_calls": provider.calls,
                    "left_inode": effect.observed_left.entries[0].directory.inode,
                    "right_inode": effect.observed_right.entries[0].directory.inode,
                }
                (root / "metadata.json").write_text(json.dumps(value))
                print(json.dumps(value), flush=True)
                assert (await asyncio.to_thread(sys.stdin.readline)).strip() == "release"
                return await original_settle(bound_scope, identifier, effect, **kwargs)

            store.settle = blocked
            try:
                await agent.execute("原生成树交换后保留和撤销", idempotency_key="original-model")
            finally:
                await service.close()
            return
        assert phase == "recover"
        value = json.loads((root / "metadata.json").read_text())
        replays = 0

        def forbidden(*_args, **_kwargs):
            nonlocal replays
            replays += 1
            raise AssertionError("恢复禁止重发Provider/命令/封存/prepare/rename/apply")

        provider.stream = forbidden
        LinuxSandboxRunner.start = forbidden
        CompletedTreeSealStore.seal = forbidden
        physical.rename_at = forbidden
        WorkspaceTreeExchangeWriter.prepare = forbidden
        WorkspaceTreeExchangeWriter.apply = forbidden
        service = ai.agents()
        service.register_workspace("files", root / "files", version="1", writable=True)
        service.register_execution("sandbox", LinuxSandboxRunner(), version="1", owned=True)
        service.register_tree_exchanges()
        assert await service.restore_persistent() == 1
        await service.recover_workspace_edits()
        store = service.manager.tree_exchanges.store
        operation = await store.operation(value["scope"], value["operation_id"])
        assert operation.view.status == "uncertain"
        assert operation.model_source == TreeExchangeModelSource(value["run_id"], value["call_id"])
        stale = WorkspaceTreeExchangeStore(service.workspace_edits)
        stale.owner_id = value["old_owner"]
        for check in (stale.assert_execution, stale.renew):
            try:
                await check(value["scope"], value["operation_id"])
            except ServiceError:
                pass
            else:
                raise AssertionError("迟到owner仍获得原发布或续租资格")
        result = await service.tree_exchange_reconcile(
            value["agent_id"], TreeExchangeOperationRequest(operation_id=value["operation_id"])
        )
        assert result.status == "done" and result.receipt.changed == (kind == "apply")
        query = await service.tree_exchange_query(
            value["agent_id"],
            TreeExchangeQueryRequest(kind="operation_by_key", request_key="model-" + kind),
        )
        assert query.operation == result
        source = (
            await service.tree_exchange_query(
                value["agent_id"],
                TreeExchangeQueryRequest(kind="by_key", request_key="model-preview"),
            )
        ).proposal
        assert (
            source.origin == "completed_tree" and source.source_plan_id == value["source_plan_id"]
        )
        binding = service._handle(value["agent_id"]).definition.workspace
        sealed = await store.seals.get(value["scope"], binding, value["source_plan_id"])
        assert sealed.origin.view.proof_hash == value["proof_hash"]
        assert sealed.origin.operation_ids == tuple(value["source_operations"])
        final = await store.operation(value["scope"], value["operation_id"])
        assert final.effect.observed_left.entries[0].directory.inode == value["left_inode"]
        assert final.effect.observed_right.entries[0].directory.inode == value["right_inode"]
        assert (root / "files/live/sub/data.bin").read_bytes() == b"\0\xff"
        assert (root / "files/tree/old.bin").read_bytes() == b"old\xff\0"
        assert provider.calls == 0 and replays == 0
        current_counts = await counts(store.database)
        assert current_counts == [value["counts"][0], value["counts"][1], 0, *value["counts"][3:]]
        print(
            json.dumps(
                {
                    "package": str(krim_agent.__file__),
                    "operation_id": result.operation_id,
                    "origin": source.origin,
                    "source_plan_id": source.source_plan_id,
                    "status": result.status,
                    "changed": result.receipt.changed,
                    "replays": replays,
                    "provider_calls": provider.calls,
                    "old_owner_fenced": True,
                    "counts": current_counts,
                    "left_inode": value["left_inode"],
                    "right_inode": value["right_inode"],
                }
            ),
            flush=True,
        )


asyncio.run(main())
