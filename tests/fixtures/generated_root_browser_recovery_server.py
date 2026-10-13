"""新浏览器消费原SQL强杀恢复；生产复用既有fixture，恢复零执行重放。"""

import asyncio
import json
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory

import uvicorn
from pydantic import SecretStr
from sqlalchemy import update

import krim_agent
import krim_agent.api.server as server_module
import krim_agent.workspace_tree_writer as physical
from krim_agent.config import Settings
from krim_agent.execution import LinuxSandboxRunner
from krim_agent.persistence.completed_tree_seals import CompletedTreeSealStore
from krim_agent.persistence.database import AgentHandleRow, Database, WorkspaceTreeMoveAttemptRow
from krim_agent.persistence.workspace_tree_moves import WorkspaceTreeMoveStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.sdk import PythonAI
from krim_agent.workspace_tree_move import TreeMoveModelSource, TreeMoveQueryRequest
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_generated_tree_undo_ledger import counts  # noqa: E402
from test_generated_tree_undo_tools import Provider  # noqa: E402


async def serve(root: Path) -> None:
    (root / "files").mkdir()
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(Path(__file__).with_name("generated_tree_undo_service_process.py")),
        "produce",
        "remove_root",
        str(root),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
    try:
        line = await asyncio.wait_for(process.stdout.readline(), 35)
        if not line:
            _, error = await process.communicate()
            raise AssertionError(error.decode())
        metadata = json.loads(line)
        assert metadata["pid"] == process.pid and metadata["provider_calls"] == 2
        process.kill()
        _, error = await asyncio.wait_for(process.communicate(), 10)
        assert process.returncode == -9, error.decode()
        # 合成受控租约到期；不冒称自然等待或生产多worker。
        async with database.sessions.begin() as sql:
            await sql.execute(
                update(AgentHandleRow)
                .where(AgentHandleRow.agent_id == metadata["agent_id"])
                .values(lease_expires_at=time.time() - 1)
            )
            await sql.execute(
                update(WorkspaceTreeMoveAttemptRow)
                .where(WorkspaceTreeMoveAttemptRow.operation_id == metadata["operation_id"])
                .values(lease_expires_at=time.time() - 1)
            )
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()
        await database.close()
    replays = 0

    def forbidden(*_args, **_kwargs):
        nonlocal replays
        replays += 1
        raise AssertionError("浏览器恢复不得重放Model/命令/seal/prepare/apply/namespace")

    Provider.stream = forbidden
    LinuxSandboxRunner.start = forbidden
    CompletedTreeSealStore.seal = forbidden
    WorkspaceTreeWriter.prepare = forbidden
    WorkspaceTreeWriter.apply = forbidden
    physical.rename_at = forbidden
    async with await PythonAI.create(
        settings=Settings(
            database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}",
            api_key=SecretStr("browser-test-key"),
        ),
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
        saved = await store.operation(metadata["scope"], metadata["operation_id"])
        assert saved.view.status == "uncertain" and saved.intent_recorded
        assert saved.model_source == TreeMoveModelSource(metadata["run_id"], metadata["call_id"])
        late = WorkspaceTreeMoveStore(service.workspace_edits)
        late.owner_id = metadata["old_owner"]
        for check in (late.assert_execution, late.renew):
            try:
                await check(metadata["scope"], metadata["operation_id"])
            except ServiceError:
                pass
            else:
                raise AssertionError("浏览器恢复允许迟到owner发布")
        view = await service.get(metadata["agent_id"])
        server_module.AgentService = lambda *_args, **_kwargs: service
        app = server_module.create_app(runtime=ai.runtime)

        @app.get("/_test/root-recovery")
        async def facts() -> dict:
            state = await service.generated_tree_undo_query(
                view.agent_id,
                TreeMoveQueryRequest(kind="proposal", identifier=metadata["proposal_id"]),
            )
            current = await store.operation(metadata["scope"], metadata["operation_id"])
            if current.view.status == "done":
                assert (
                    current.effect.observed_after.entries[0].directory.inode
                    == metadata["root_inode"]
                )
                assert (
                    current.effect.observed_after.entries[-1].file.inode == metadata["file_inode"]
                )
            binary = root / "files/retained/sub/data.bin"
            assert binary.stat().st_ino == metadata["file_inode"]
            assert (root / "files/retained").stat().st_ino == metadata["root_inode"]
            assert binary.read_bytes() == b"\0\xff" and replays == 0
            return {
                **metadata,
                "topic_id": view.topic_id,
                "package": str(krim_agent.__file__),
                "operation_status": current.view.status,
                "root_state": state.proposal.root_state,
                "generation_status": state.proposal.generation_status,
                "replay_calls": replays,
                "old_owner_fenced": True,
                "binary_hex": binary.read_bytes().hex(),
                "counts": await counts(store.database),
            }

        server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=8877, log_level="error"))
        await server.serve()


with TemporaryDirectory(prefix="krim-root-browser-recovery-", dir="/data/tmp/root") as folder:
    asyncio.run(serve(Path(folder)))
