"""真实回退强杀后，新服务/browser/SDK/CLI同原事实，原生产入口禁用。"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory

import uvicorn
from pydantic import SecretStr
from sqlalchemy import select, update

import krim_agent
import krim_agent.api.server as server_module
import krim_agent.workspace_tree_writer as physical
from krim_agent.config import Settings
from krim_agent.persistence import Database
from krim_agent.persistence.database import (
    AgentHandleRow,
    WorkspaceTreeMoveAttemptRow,
    WorkspaceTreeMoveModelSourceRow,
)
from krim_agent.providers import FakeProvider
from krim_agent.sdk import AgentSession, MissingDirectoryRollbackQuery, PythonAI
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

if os.environ.get("KRIM_REQUIRE_INSTALLED_PACKAGE") == "1":
    assert "/site-packages/" in str(krim_agent.__file__), krim_agent.__file__

REPLAYS = []


class NoProvider(FakeProvider):
    def __init__(self):
        super().__init__(delay=0)
        self.calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        REPLAYS.append(True)
        raise AssertionError("浏览器冷恢复不得重放原Provider")
        yield


async def serve(root):
    point = os.environ.get("KRIM_TEST_MISSING_ROLLBACK_RECOVERY_POINT", "namespace")
    origin = "model" if point == "namespace" else "host"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(Path(__file__).with_name("missing_baseline_rollback_control_process.py")),
        "produce",
        origin,
        point,
        str(root),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
    try:
        line = await asyncio.wait_for(process.stdout.readline(), 25)
        if not line:
            _, error = await process.communicate()
            raise AssertionError(error.decode())
        signal = json.loads(line)
        assert signal["pid"] == process.pid and signal["point"] == point
        inode = (root / "files/preserved").stat().st_ino
        process.kill()
        _, error = await asyncio.wait_for(process.communicate(), 5)
        assert process.returncode == -9, error.decode()
        metadata = json.loads((root / "metadata.json").read_text())
        async with database.sessions() as sql, sql.begin():
            await sql.execute(
                update(AgentHandleRow)
                .where(AgentHandleRow.agent_id == metadata["agent_id"])
                .values(lease_expires_at=time.time() - 1)
            )
            row = await sql.get(WorkspaceTreeMoveAttemptRow, metadata["operation_id"])
            deadline = row.lease_expires_at
        if deadline is not None:
            await asyncio.sleep(max(0, deadline - time.time()) + 0.05)
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()
        await database.close()
    replays = REPLAYS

    def forbidden(*_args, **_kwargs):
        replays.append(True)
        raise AssertionError("浏览器冷恢复不得重放Provider/prepare/apply/rename")

    WorkspaceTreeWriter.prepare = forbidden
    WorkspaceTreeWriter.apply = forbidden
    physical.rename_at = forbidden
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}",
        api_key=SecretStr("browser-test-key"),
    )
    async with await PythonAI.create(settings=settings, provider=NoProvider()) as ai:

        async def policy(definition, _context):
            return "allow" if definition.risk == "read" else "ask"

        ai.runtime.tool_policy = policy
        service = ai.agents()
        service.register_workspace(
            "files", root / "files", version="1", writable=True, default=True
        )
        service.register_directory_checkpoints()
        service.register_missing_directory_restores()
        service.register_missing_directory_rollbacks()
        assert await service.restore_persistent() == 1, service.restore_errors
        view = await service.get(metadata["agent_id"])
        agent = AgentSession(service, view.agent_id)
        server_module.AgentService = lambda *_args, **_kwargs: service
        app = server_module.create_app(runtime=ai.runtime)

        @app.get("/_test/missing-rollback-recovery")
        async def facts(consumers: bool = False):
            current = await agent.query_missing_directory_rollback(
                MissingDirectoryRollbackQuery(kind="operation", identifier=metadata["operation_id"])
            )
            proposal = (
                await agent.query_missing_directory_rollback(
                    MissingDirectoryRollbackQuery(
                        kind="proposal", identifier=metadata["proposal_id"]
                    )
                )
            ).proposal
            async with service.workspace_edits.database.sessions() as sql:
                sources = list(
                    await sql.scalars(
                        select(WorkspaceTreeMoveModelSourceRow).where(
                            WorkspaceTreeMoveModelSourceRow.proposal_id == metadata["proposal_id"]
                        )
                    )
                )
            if origin == "model":
                assert {(item.run_id, item.call_id) for item in sources} == {
                    (metadata["model_run_id"], "original-preview"),
                    (metadata["model_run_id"], "original-change"),
                }
            assert (root / "files/preserved").stat().st_ino == inode
            assert (
                current.operation.source_plan_id == metadata["source_plan_id"]
                and current.operation.source_operation_id == metadata["source_operation_id"]
            )
            if consumers:
                from test_missing_directory_rollback_public_consumers import cli

                from krim_agent.client import KrimClient

                def read():
                    with KrimClient("http://127.0.0.1:8879", "browser-test-key") as client:
                        return client.agent_missing_directory_rollback_query(
                            view.agent_id,
                            MissingDirectoryRollbackQuery(
                                kind="operation_by_key", request_key="rollback-apply"
                            ),
                        )

                assert await asyncio.to_thread(read) == current.model_copy(
                    update={"kind": "operation_by_key"}
                )
                result = await cli(
                    "http://127.0.0.1:8879",
                    view.agent_id,
                    "query",
                    None,
                    "--kind",
                    "operation_by_key",
                    "--request-key",
                    "rollback-apply",
                )
                assert result["operation"] == current.operation.model_dump(mode="json")
            return {
                "agent_id": view.agent_id,
                "topic_id": view.topic_id,
                "operation_id": metadata["operation_id"],
                "source_plan_id": metadata["source_plan_id"],
                "source_operation_id": metadata["source_operation_id"],
                "proposal_id": metadata["proposal_id"],
                "status": current.operation.operation.status,
                "state": proposal.rollback_state,
                "inode": inode,
                "binary_hex": (root / "files/preserved/nested/nul").read_bytes().hex(),
                "replays": len(replays),
                "natural_move_lease_expiry": deadline is not None,
                "original_model_source": origin == "model",
                "consumers_same": consumers,
                "point": point,
                "package": str(krim_agent.__file__),
            }

        await uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=8879, log_level="error")
        ).serve()


with TemporaryDirectory(prefix="krim-missing-rollback-browser-", dir="/data/tmp/root") as directory:
    asyncio.run(serve(Path(directory)))
