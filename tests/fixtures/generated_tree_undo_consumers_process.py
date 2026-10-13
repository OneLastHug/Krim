"""原真正Model效果后恢复，同SDK/HTTP/TCP/CLI读取原SQL生成根事实。"""

import asyncio
import json
import sys
from pathlib import Path

import httpx

import krim_agent
import krim_agent.api.server as server_module
import krim_agent.workspace_tree_writer as physical
from krim_agent.config import Settings
from krim_agent.execution import LinuxSandboxRunner
from krim_agent.persistence.completed_tree_seals import CompletedTreeSealStore
from krim_agent.persistence.workspace_tree_moves import WorkspaceTreeMoveStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import AgentSession, PythonAI
from krim_agent.workspace_tree_move import TreeMoveModelSource, TreeMoveQueryRequest
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_generated_tree_undo_public_consumers import cli  # noqa: E402
from test_generated_tree_undo_tools import Provider  # noqa: E402
from test_tree_move_public_consumers import tcp_for  # noqa: E402


async def main():
    root = Path(sys.argv[1])
    value = json.loads((root / "metadata.json").read_text())
    replays = 0

    def forbidden(*_args, **_kwargs):
        nonlocal replays
        replays += 1
        raise AssertionError("管理恢复不得重放原Model/命令/seal/prepare/apply/namespace")

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

        async def policy(definition, _context):
            return "allow" if definition.risk == "read" else "ask"

        ai.runtime.tool_policy = policy
        service = ai.agents()
        service.register_workspace("files", root / "files", version="1", writable=True)
        service.register_execution("sandbox", LinuxSandboxRunner(), version="1", owned=True)
        service.register_generated_tree_undos()
        assert await service.restore_persistent() == 1, service.restore_errors
        store = service.manager.tree_moves.store
        pending = await store.operation(value["scope"], value["operation_id"])
        assert pending.view.status == "uncertain" and pending.intent_recorded
        assert pending.model_source == TreeMoveModelSource(value["run_id"], value["call_id"])
        stale = WorkspaceTreeMoveStore(service.workspace_edits)
        stale.owner_id = value["old_owner"]
        for operation in (stale.assert_execution, stale.renew):
            try:
                await operation(value["scope"], value["operation_id"])
            except ServiceError:
                pass
            else:
                raise AssertionError("管理恢复允许迟到owner发布")
        server_module.AgentService = lambda *_args, **_kwargs: service
        app = server_module.create_app(runtime=ai.runtime)
        base = f"/v1/agents/{value['agent_id']}/workspace"
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api:
            response = await api.post(
                base + f"/generated-tree-undo-operations/{value['operation_id']}/reconcile",
                params={"confirmed": "true"},
            )
            assert response.status_code == 200, response.text
            actual = response.json()
            assert (
                actual["source_plan_id"] == value["plan_id"]
                and actual["operation"]["operation_id"] == value["operation_id"]
            )
            assert actual["operation"]["status"] == "done"
            agent = AgentSession(service, value["agent_id"])
            sdk = await agent.query_generated_tree_undo(
                TreeMoveQueryRequest(kind="operation", identifier=value["operation_id"])
            )
            assert sdk.operation.model_dump(mode="json") == actual
            async with tcp_for(app) as url:
                from krim_agent.client import KrimClient

                with KrimClient(url) as client:
                    tcp = await asyncio.to_thread(
                        client.agent_generated_tree_undo_query,
                        value["agent_id"],
                        TreeMoveQueryRequest(kind="operation", identifier=value["operation_id"]),
                    )
                    assert tcp == sdk
                assert (
                    await cli(url, value["agent_id"], "operation", value["operation_id"])
                ) == sdk.model_dump(mode="json")
                repeated = await cli(
                    url, value["agent_id"], "reconcile", value["operation_id"], "--confirm"
                )
                assert repeated == actual
            stored = await store.operation(value["scope"], value["operation_id"])
            assert stored.effect.observed_after.entries[0].directory.inode == value["root_inode"]
            assert stored.effect.observed_after.entries[-1].file.inode == value["file_inode"]
            assert (root / "files/retained/sub/data.bin").read_bytes() == b"\0\xff"
            assert replays == 0
            print(
                json.dumps(
                    {
                        "package": str(krim_agent.__file__),
                        "operation_id": value["operation_id"],
                        "plan_id": value["plan_id"],
                        "source_preserved": True,
                        "root_inode": value["root_inode"],
                        "file_inode": value["file_inode"],
                        "replay_calls": replays,
                        "consumers_same_fact": True,
                        "old_owner_fenced": True,
                    }
                ),
                flush=True,
            )


asyncio.run(main())
