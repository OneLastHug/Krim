"""项目外安装包的混合SDK后台执行，以及真实TCP Client/CLI消费。"""

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
from krim_agent.tools.builtins import builtin_registry
from krim_agent.workspace_change import FileChangeSpec, WorkspacePreviewRequest
from krim_agent.workspace_directory import DirectoryChangeSpec, DirectoryPreviewRequest
from krim_agent.workspace_mixed_batch import MixedBatchRequest, MixedBatchTarget


async def main():
    assert "/site-packages/" in str(krim_agent.__file__)
    with TemporaryDirectory(prefix="krim-mixed-worker-wheel-", dir="/data/tmp/root") as folder:
        root = Path(folder)
        files = root / "files"
        files.mkdir()

        async def policy(definition, _context):
            return "allow" if definition.risk == "read" else "ask"

        database = Database(f"sqlite+aiosqlite:///{root / 'state.sqlite'}")
        await database.initialize()
        runtime = ChatRuntime(
            KrimRepository(database),
            FakeProvider(delay=0),
            builtin_registry(),
            Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        )
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

            def prepare():
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
                    request = MixedBatchRequest(
                        action="apply",
                        request_key="apply",
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
                    accepted = client.agent_workspace_start_mixed_batch(
                        agent, request, confirmed=True
                    )
                    done = client.agent_workspace_wait_mixed_batch(agent, accepted.batch_id)
                    assert done.status == "done", done.model_dump_json()
                    assert client.agent_workspace_mixed_batch(agent, done.batch_id) == done
                    assert (
                        client.agent_workspace_start_mixed_batch(agent, request, confirmed=True)
                        == done
                    )
                    return agent, done

            agent, applied = await asyncio.to_thread(prepare)
            environment = {
                key: value for key, value in os.environ.items() if not key.startswith("KRIM_AGENT_")
            }

            async def cli(*args):
                child = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-I",
                    "-m",
                    "krim_agent",
                    "workspace",
                    *map(str, args),
                    "--url",
                    url,
                    env=environment,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                try:
                    stdout, stderr = await asyncio.wait_for(child.communicate(), 30)
                    assert child.returncode == 0, stderr.decode()
                    return json.loads(stdout)
                finally:
                    if child.returncode is None:
                        child.kill()
                        await child.communicate()

            assert (await cli("mixed-batch-by-key", agent, "--request-key", "apply"))[
                "batch_id"
            ] == applied.batch_id
            for action in ("keep", "undo"):

                def revisions():
                    with KrimClient(url) as client:
                        file = client.agent_workspace_proposal(agent, applied.items[0].proposal_id)
                        directory = client.agent_directory_query(
                            agent, kind="proposal", identifier=applied.items[1].proposal_id
                        ).proposal
                        return [file.revision, directory.revision]

                current = await asyncio.to_thread(revisions)
                request = MixedBatchRequest(
                    action=action,
                    request_key=action,
                    targets=[
                        MixedBatchTarget(
                            source=item.source,
                            proposal_id=item.proposal_id,
                            expected_revision=revision,
                        )
                        for item, revision in zip(applied.items, current, strict=True)
                    ],
                )
                path = root / (action + ".json")
                path.write_text(request.model_dump_json())
                stamp = (
                    [(files / path).stat() for path in ("file.txt", "empty")]
                    if action == "keep"
                    else None
                )
                accepted = await cli("start-mixed-batch", agent, "--request", path, "--confirm")
                done = await cli("wait-mixed-batch", agent, "--batch-id", accepted["batch_id"])
                assert done["status"] == "done"
                if stamp is not None:
                    assert stamp == [(files / path).stat() for path in ("file.txt", "empty")]
            assert not (files / "file.txt").exists() and not (files / "empty").exists()
            async with app.state.runtime.repository.database.sessions() as sql:
                assert (
                    await sql.scalar(select(func.count()).select_from(WorkspaceEditAttemptRow)) == 3
                )
                assert (
                    await sql.scalar(select(func.count()).select_from(WorkspaceDirectoryAttemptRow))
                    == 3
                )
            print(
                json.dumps(
                    {
                        "package": krim_agent.__file__,
                        "real_tcp_client": True,
                        "real_cli": True,
                        "mixed_apply_keep_undo": True,
                        "keep_zero_write": True,
                        "same_key_zero_replay": True,
                        "file_attempts": 3,
                        "directory_attempts": 3,
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
