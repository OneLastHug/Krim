"""独立服务进程与安装包：真实TCP客户端、原操作重读和撤销。"""

import asyncio
import json
import socket
import subprocess
import sys
from pathlib import Path

import uvicorn

import krim_agent
from krim_agent.api import create_app
from krim_agent.client import KrimClient
from krim_agent.config import Settings
from krim_agent.workspace_change import (
    FileChangeSpec,
    WorkspaceChangeAction,
    WorkspacePreviewRequest,
)


async def main():
    database, root, mode, metadata_path = sys.argv[1:5]
    app = create_app(
        Settings(database_url=f"sqlite+aiosqlite:///{database}", interactive_tools=True),
        workspace_root=root,
        workspace_version="v1",
        workspace_writable=True,
    )
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        # 数据库迁移/资源恢复的观察窗口，不是产品启动SLA或文件执行预算。
        async with asyncio.timeout(30):
            while not server.started:
                if task.done():
                    await task
                await asyncio.sleep(0.01)

        def check():
            with KrimClient(f"http://127.0.0.1:{port}") as client:
                metadata = Path(metadata_path)
                if mode == "create":
                    agent = client.agent_spawn("default", persistent=True)
                    before = client.agent_read_file(agent.agent_id, "file.txt")
                    proposal = client.agent_workspace_preview(
                        agent.agent_id,
                        WorkspacePreviewRequest(
                            spec=FileChangeSpec(
                                path="file.txt",
                                operation="write",
                                expected_sha256=before.sha256,
                                content="new\n",
                            ),
                            request_key="preview",
                        ),
                    )
                    request = WorkspaceChangeAction(
                        proposal_id=proposal.proposal_id,
                        action="apply",
                        expected_revision=proposal.revision,
                        request_key="apply",
                    )
                    result = client.agent_workspace_change(agent.agent_id, request, confirmed=True)
                    metadata.write_text(
                        json.dumps(
                            {
                                "agent_id": agent.agent_id,
                                "request": request.model_dump(),
                                "operation_id": result.operation_id,
                            }
                        )
                    )
                    identifier = agent.agent_id
                    assert Path(root, "file.txt").read_text() == "new\n"
                else:
                    original = json.loads(metadata.read_text())
                    identifier = original["agent_id"]
                    request = WorkspaceChangeAction.model_validate(original["request"])
                    assert client.agent_inventory()[0].available
                    result = client.agent_workspace_change(identifier, request, confirmed=True)
                    assert result.operation_id == original["operation_id"]
                    assert (
                        client.agent_workspace_operation(identifier, result.operation_id).status
                        == "done"
                    )
                    current = client.agent_workspace_proposal(identifier, request.proposal_id)
                    assert (
                        client.agent_workspace_changes(identifier)[0].proposal_id
                        == current.proposal_id
                    )
                    assert len(client.agent_workspace_history(identifier, current.proposal_id)) == 1
                    assert (
                        client.agent_workspace_reconcile(
                            identifier, result.operation_id, confirmed=True
                        ).status
                        == "done"
                    )
                    assert not client.agent_workspace_finalize(
                        identifier, result.operation_id, confirmed=True
                    ).cleanup_pending
                    undone = client.agent_workspace_change(
                        identifier,
                        WorkspaceChangeAction(
                            proposal_id=current.proposal_id,
                            action="undo",
                            expected_revision=current.revision,
                            request_key="undo",
                        ),
                        confirmed=True,
                    )
                    assert undone.status == "done"
                    assert not client.agent_workspace_finalize(
                        identifier, undone.operation_id, confirmed=True
                    ).cleanup_pending
                    assert Path(root, "file.txt").read_text() == "old\n"
                    assert not list(Path(root).glob("._krim-tx-*"))
                assert result.status == "done"
                command = subprocess.run(
                    [
                        sys.executable,
                        "-I",
                        "-m",
                        "krim_agent",
                        "workspace",
                        "operation",
                        identifier,
                        result.operation_id,
                        "--url",
                        f"http://127.0.0.1:{port}",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=15,
                )
                assert command.returncode == 0, command.stderr
                assert json.loads(command.stdout)["operation_id"] == result.operation_id
                return {
                    "package": krim_agent.__file__,
                    "agent_id": identifier,
                    "operation_id": result.operation_id,
                    "status": result.status,
                    "cli_operation_verified": True,
                }

        value = await asyncio.to_thread(check)
        assert not await app.state.runtime.repository.active_runs()
        print(json.dumps(value))
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 30)
        listener.close()


asyncio.run(main())
