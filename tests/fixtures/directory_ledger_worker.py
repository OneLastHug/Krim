"""目录账本真实进程保存/核实，仅临时DB和可核验原intent。"""

import asyncio
import json
import os
import sys
from pathlib import Path

from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.workspace_directories import WorkspaceDirectoryStore
from krim_agent.workspace import Workspace
from krim_agent.workspace_directory import (
    DirectoryChangeAction,
    DirectoryChangeSpec,
    WorkspaceDirectoryWriter,
)


async def main():
    folder, mode = sys.argv[1:]
    root = Path(folder)
    files = root / "files"
    files.mkdir(exist_ok=True)
    database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
    await database.initialize()
    try:
        store = WorkspaceDirectoryStore(database)
        with Workspace(files, writable=True) as workspace:
            writer = WorkspaceDirectoryWriter(workspace)
            if mode == "write":
                scope = (
                    "topic:"
                    + (await KrimRepository(database).create_topic("directory durable process")).id
                )
                spec = DirectoryChangeSpec(path="empty", operation="create", mode=0o750)
                plan = writer.prepare(spec)
                proposal, _ = await store.preview(
                    scope, "files", workspace.fingerprint, "preview", plan
                )
                action = DirectoryChangeAction(
                    proposal_id=proposal.view.proposal_id,
                    action="apply",
                    expected_revision=0,
                    request_key="apply",
                )
                attempt, _ = await store.reserve(scope, action, workspace.fingerprint, plan)
                loop = asyncio.get_running_loop()

                def record(reference):
                    asyncio.run_coroutine_threadsafe(
                        store.record_intent(scope, attempt.view.operation_id, reference), loop
                    ).result(timeout=5)

                def authorize():
                    asyncio.run_coroutine_threadsafe(
                        store.assert_execution(scope, attempt.view.operation_id), loop
                    ).result(timeout=5)

                effect = await asyncio.to_thread(
                    writer.apply, plan, record_intent=record, authorize=authorize
                )
                assert effect.receipt.status == "done"
                # 刻意不settle：模拟系统调用成功后生产进程直接退出。
                (root / "state.json").write_text(
                    json.dumps(
                        {
                            "scope": scope,
                            "operation_id": attempt.view.operation_id,
                            "proposal_id": proposal.view.proposal_id,
                        }
                    )
                )
                os._exit(23)
            state = json.loads((root / "state.json").read_text())
            operation = await store.operation(state["scope"], state["operation_id"])
            assert operation.view.status == "uncertain"
            effect = writer.verify(operation.prepared, operation.recovery)
            result = await store.settle(
                state["scope"], operation.view.operation_id, effect, resolving=True
            )
            assert result.view.status == "done"
            print(
                json.dumps(
                    {
                        "status": result.view.status,
                        "mode": writer.capture("empty").mode,
                        "operation_id": result.view.operation_id,
                    }
                )
            )
    finally:
        await database.close()


asyncio.run(main())
