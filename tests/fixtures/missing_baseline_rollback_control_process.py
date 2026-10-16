"""真实受控Host/模型效果与回执两窗口强杀，恢复消费产品SQL而不重放。"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path

from sqlalchemy import select

import krim_agent
import krim_agent.workspace_tree_writer as physical
from krim_agent.config import Settings
from krim_agent.persistence.database import (
    WorkspaceTreeMoveAttemptRow,
    WorkspaceTreeMoveModelSourceRow,
)
from krim_agent.providers import FakeProvider
from krim_agent.sdk import PythonAI
from krim_agent.workspace_missing_directory_rollback_control import MissingDirectoryRollbackQuery
from krim_agent.workspace_tree_move import TreeMoveOperationRequest
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_missing_baseline_rollback_control import Provider, action, setup  # noqa: E402


class NoProvider(FakeProvider):
    def __init__(self):
        super().__init__(delay=0)
        self.calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        raise AssertionError("恢复不得重跑模型")
        yield


async def main():
    phase, origin, point, directory = sys.argv[1:5]
    root = Path(directory)
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=NoProvider(),
    ) as ai:
        if phase == "produce":
            (root / "files").mkdir()
            service, agent, _, source, request = await setup(
                ai.runtime, root / "files", persistent=True
            )
            store = service.manager.tree_moves.store
            store.lease_seconds = 5
            provider = Provider(request) if origin == "model" else None
            if provider is not None:
                service._handle(agent.agent_id).runtime.provider = provider
            scope = "session:" + agent.session_id

            async def signal():
                operation = await store.operation_by_key(scope, "rollback-apply")
                async with store.database.sessions() as sql:
                    row = await sql.get(WorkspaceTreeMoveAttemptRow, operation.view.operation_id)
                    lease = row.lease_expires_at
                metadata = {
                    "agent_id": agent.agent_id,
                    "session_id": agent.session_id,
                    "source_plan_id": source.plan_id,
                    "source_operation_id": source.operation_id,
                    "proposal_id": operation.view.proposal_id,
                    "operation_id": operation.view.operation_id,
                    "lease": lease,
                    "old_owner": store.owner_id,
                    "origin": origin,
                    "provider_calls": provider.calls if provider is not None else 0,
                    "model_run_id": operation.model_source.run_id
                    if operation.model_source is not None
                    else None,
                }
                with (root / "metadata.json").open("w") as output:
                    json.dump(metadata, output)
                    output.flush()
                    os.fsync(output.fileno())
                print(
                    json.dumps(
                        {
                            "pid": os.getpid(),
                            "point": point,
                            "origin": origin,
                            "package": str(krim_agent.__file__),
                        }
                    ),
                    flush=True,
                )

            if point == "namespace":
                apply = WorkspaceTreeWriter.apply
                loop = asyncio.get_running_loop()

                def held(writer, change, **kwargs):
                    effect = apply(writer, change, **kwargs)
                    assert effect.receipt.status == "done"
                    asyncio.run_coroutine_threadsafe(signal(), loop).result(timeout=5)
                    sys.stdin.readline()
                    raise AssertionError("须由父进程真实SIGKILL")

                WorkspaceTreeWriter.apply = held
            else:
                settle = store.settle

                async def held(*args, **kwargs):
                    value = await settle(*args, **kwargs)
                    assert value.view.status == "done"
                    await signal()
                    await asyncio.to_thread(sys.stdin.readline)
                    raise AssertionError("须由父进程真实SIGKILL")

                store.settle = held
            if provider is not None:
                await agent.execute("原模型缺失回退强杀", idempotency_key="original-model")
            else:
                proposed = await service.missing_directory_rollback_preview(agent.agent_id, request)
                await service.missing_directory_rollback_change(agent.agent_id, action(proposed))
            raise AssertionError("生产进程未到达真实强杀窗口")

        metadata = json.loads((root / "metadata.json").read_text())

        async def restored_policy(definition, context):
            if context.agent_id != metadata["agent_id"]:
                return "deny"
            if definition.name == "missing_directory_rollback_reconcile" and context.arguments == {
                "operation_id": metadata["operation_id"]
            }:
                return "ask"
            return "allow" if definition.risk == "read" else "deny"

        ai.runtime.tool_policy = restored_policy
        service = ai.agents()
        service.register_workspace(
            "files", root / "files", version="1", writable=True, default=True
        )
        service.register_directory_checkpoints()
        service.register_missing_directory_restores()
        service.register_missing_directory_rollbacks()
        store = service.manager.tree_moves.store
        async with store.database.sessions() as sql:
            row = await sql.get(WorkspaceTreeMoveAttemptRow, metadata["operation_id"])
            deadline = row.lease_expires_at
        if deadline is not None:
            await asyncio.sleep(max(0, deadline - time.time()) + 0.05)
        replays = []

        def forbidden(*_args, **_kwargs):
            replays.append(True)
            raise AssertionError("恢复禁止prepare/apply/rename重放")

        WorkspaceTreeWriter.apply = forbidden
        WorkspaceTreeWriter.prepare = forbidden
        physical.rename_at = forbidden
        assert await service.restore_persistent() == 1
        assert await service.restore_persistent() == 0
        actual = await service.missing_directory_rollback_reconcile(
            metadata["agent_id"],
            TreeMoveOperationRequest(operation_id=metadata["operation_id"]),
            confirmed=True,
        )
        assert (
            actual.operation.status == "done"
            and actual.operation.operation_id == metadata["operation_id"]
        )
        query = await service.missing_directory_rollback_query(
            metadata["agent_id"],
            MissingDirectoryRollbackQuery(kind="proposal", identifier=metadata["proposal_id"]),
        )
        assert (
            query.proposal.rollback_state == "preserved"
            and query.proposal.source_plan_id == metadata["source_plan_id"]
        )
        assert query.proposal.source_operation_id == metadata["source_operation_id"]
        async with store.database.sessions() as sql:
            sources = list(
                await sql.scalars(
                    select(WorkspaceTreeMoveModelSourceRow).where(
                        WorkspaceTreeMoveModelSourceRow.proposal_id == metadata["proposal_id"]
                    )
                )
            )
        assert len(sources) == (2 if origin == "model" else 0)
        if origin == "model":
            assert {(item.run_id, item.call_id) for item in sources} == {
                (metadata["model_run_id"], "original-preview"),
                (metadata["model_run_id"], "original-change"),
            }
        assert not replays and ai.runtime.provider.calls == 0
        assert (root / "files/preserved/nested/nul").read_bytes() == b"user\0modified\xff"
        assert not (root / "files/tree").exists()
        print(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "operation_id": actual.operation.operation_id,
                    "source_plan_id": actual.source_plan_id,
                    "provider_calls": 0,
                    "replays": 0,
                    "natural_move_lease_expiry": deadline is not None,
                    "package": str(krim_agent.__file__),
                }
            ),
            flush=True,
        )


asyncio.run(main())
