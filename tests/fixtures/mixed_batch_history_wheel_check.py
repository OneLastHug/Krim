"""项目外安装包的原混合历史SDK/TCP Client/CLI元数据分页与零效果。"""

import asyncio
import json
import os
import socket
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import uvicorn
from sqlalchemy import func, select

import krim_agent
from krim_agent.api import create_app
from krim_agent.client import KrimClient
from krim_agent.config import Settings
from krim_agent.persistence import Database, KrimRepository
from krim_agent.persistence.database import WorkspaceDirectoryAttemptRow, WorkspaceEditAttemptRow
from krim_agent.providers import FakeProvider
from krim_agent.runtime import ChatRuntime
from krim_agent.sdk import AgentSession
from krim_agent.tools.builtins import builtin_registry
from krim_agent.workspace_change import FileChangeSpec, WorkspacePreviewRequest
from krim_agent.workspace_directory import DirectoryChangeSpec, DirectoryPreviewRequest
from krim_agent.workspace_mixed_batch import MixedBatchRequest, MixedBatchTarget


async def main():
    assert "/site-packages/" in str(krim_agent.__file__)
    with TemporaryDirectory(prefix="krim-mixed-history-wheel-", dir="/data/tmp/root") as folder:
        root = Path(folder)
        files = root / "files"
        files.mkdir()
        database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
        await database.initialize()
        runtime = ChatRuntime(
            KrimRepository(database),
            FakeProvider(delay=0),
            builtin_registry(),
            Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        )

        async def policy(definition, _context):
            return "allow" if definition.risk == "read" else "ask"

        runtime.tool_policy = policy
        app = create_app(runtime=runtime, workspace_root=files, workspace_writable=True)
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        listener.setblocking(False)
        url = f"http://127.0.0.1:{listener.getsockname()[1]}"
        server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
        task = asyncio.create_task(server.serve(sockets=[listener]))
        try:
            async with asyncio.timeout(20):
                while not server.started:
                    if task.done():
                        await task
                    await asyncio.sleep(0.01)

            def create():
                with KrimClient(url) as client:
                    agent = client.agent_spawn("default").agent_id
                    file = client.agent_workspace_preview(
                        agent,
                        WorkspacePreviewRequest(
                            spec=FileChangeSpec(
                                path="file.txt",
                                operation="write",
                                content="data",
                                expected_sha256=None,
                            ),
                            request_key="file",
                        ),
                    )
                    directory = client.agent_directory_preview(
                        agent,
                        DirectoryPreviewRequest(
                            spec=DirectoryChangeSpec(path="empty", operation="create", mode=0o750),
                            request_key="directory",
                        ),
                    )
                    identifiers = []
                    for index in range(3):
                        request = MixedBatchRequest(
                            action="apply",
                            request_key=f"batch-{index}",
                            targets=[
                                MixedBatchTarget(
                                    source="file", proposal_id=file.proposal_id, expected_revision=0
                                ),
                                MixedBatchTarget(
                                    source="directory",
                                    proposal_id=directory.proposal_id,
                                    expected_revision=0,
                                ),
                            ],
                        )
                        accepted = client.agent_workspace_start_mixed_batch(agent, request)
                        denied = client.agent_workspace_wait_mixed_batch(agent, accepted.batch_id)
                        assert denied.status == "failed" and all(
                            item.operation is None for item in denied.items
                        )
                        identifiers.append(accepted.batch_id)
                    first = client.agent_workspace_mixed_batch_history(agent, limit=2)
                    second = client.agent_workspace_mixed_batch_history(
                        agent, limit=2, cursor=first.next_cursor
                    )
                    assert first.total == second.total == 3
                    assert [item.batch_id for item in first.items + second.items] == list(
                        reversed(identifiers)
                    )
                    assert all(
                        item.done_count == 0 and item.item_count == 2 and item.status == "failed"
                        for item in first.items + second.items
                    )
                    return agent, first

            agent, first = await asyncio.to_thread(create)
            sdk = AgentSession(app.state.agents, agent)
            assert await sdk.mixed_change_batch_history(limit=2) == first
            environment = {
                key: value for key, value in os.environ.items() if not key.startswith("KRIM_AGENT_")
            }
            child = await asyncio.create_subprocess_exec(
                sys.executable,
                "-I",
                "-m",
                "krim_agent",
                "workspace",
                "mixed-batch-history",
                agent,
                "--limit",
                "2",
                "--url",
                url,
                env=environment,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(child.communicate(), 30)
                assert child.returncode == 0, stderr.decode()
                assert json.loads(stdout) == first.model_dump(mode="json")
            finally:
                if child.returncode is None:
                    child.kill()
                    await child.communicate()
            async with database.sessions() as sql:
                assert (
                    await sql.scalar(select(func.count()).select_from(WorkspaceEditAttemptRow)) == 0
                )
                assert (
                    await sql.scalar(select(func.count()).select_from(WorkspaceDirectoryAttemptRow))
                    == 0
                )
            assert not list(files.iterdir())
            print(
                json.dumps(
                    {
                        "package": krim_agent.__file__,
                        "sdk_tcp_cli_history": True,
                        "frozen_upper_paging": True,
                        "original_parent_ids": True,
                        "zero_original_attempts": True,
                        "zero_file_effects": True,
                    }
                )
            )
        finally:
            server.should_exit = True
            await asyncio.wait_for(task, 30)
            listener.close()
            await runtime.close()
            await database.close()


asyncio.run(main())
