"""独立HTTP服务进程、客户端和CLI的原生检查点恢复演练。"""

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
from krim_agent.sdk import (
    CheckpointRestoreExecute,
    CheckpointRestorePreview,
    FileChangeSpec,
    WorkspaceChangeAction,
    WorkspaceCheckpointCreate,
    WorkspacePreviewRequest,
)


async def main():
    directory, mode = Path(sys.argv[1]), sys.argv[2]
    root = directory / "files"
    metadata_file = directory / "metadata.json"
    app = create_app(
        Settings(
            database_url=f"sqlite+aiosqlite:///{directory / 'api.sqlite'}", interactive_tools=True
        ),
        workspace_root=root,
        workspace_version="v1",
        workspace_writable=True,
    )
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    url = f"http://127.0.0.1:{listener.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(30):
            while not server.started:
                if task.done():
                    await task
                await asyncio.sleep(0.01)

        def check():
            with KrimClient(url) as client:
                if mode == "create":
                    identifier = client.agent_spawn("default", persistent=True).agent_id
                    capture_request = WorkspaceCheckpointCreate(
                        paths=["file.txt", "absent.txt"], request_key="capture/原始?key#"
                    )
                    checkpoint = client.agent_workspace_create_checkpoint(
                        identifier, capture_request
                    )
                    checkpoint = client.agent_workspace_wait_checkpoint(
                        identifier, checkpoint.checkpoint_id
                    )
                    assert checkpoint.status == "available"
                    before = client.agent_read_file(identifier, "file.txt")
                    proposal = client.agent_workspace_preview(
                        identifier,
                        WorkspacePreviewRequest(
                            spec=FileChangeSpec(
                                path="file.txt",
                                operation="write",
                                expected_sha256=before.sha256,
                                content="B",
                            ),
                            request_key="change-preview",
                        ),
                    )
                    result = client.agent_workspace_change(
                        identifier,
                        WorkspaceChangeAction(
                            proposal_id=proposal.proposal_id,
                            action="apply",
                            expected_revision=proposal.revision,
                            request_key="change-apply",
                        ),
                        confirmed=True,
                    )
                    assert result.status == "done"
                    request = CheckpointRestorePreview(
                        checkpoint_id=checkpoint.checkpoint_id, request_key="restore/原始?key#"
                    )
                    plan = client.agent_workspace_preview_restore(identifier, request)
                    plan = client.agent_workspace_wait_restore(identifier, plan.plan_id)
                    assert plan.phase == "ready", plan
                    execute = CheckpointRestoreExecute(
                        plan_id=plan.plan_id,
                        expected_revision=plan.revision,
                        request_key="restore-execute",
                    )
                    metadata = {
                        "agent_id": identifier,
                        "checkpoint_id": checkpoint.checkpoint_id,
                        "plan_id": plan.plan_id,
                        "preview": request.model_dump(mode="json"),
                        "execute": execute.model_dump(mode="json"),
                        "capture": capture_request.model_dump(mode="json"),
                    }
                    metadata_file.write_text(json.dumps(metadata))
                    assert (root / "file.txt").read_text() == "B"
                else:
                    metadata = json.loads(metadata_file.read_text())
                    identifier = metadata["agent_id"]
                    assert client.agent_inventory()[0].available
                    checkpoint = client.agent_workspace_checkpoint(
                        identifier, metadata["checkpoint_id"]
                    )
                    assert checkpoint.status == "available"
                    assert (
                        client.agent_workspace_create_checkpoint(
                            identifier,
                            WorkspaceCheckpointCreate.model_validate(metadata["capture"]),
                        )
                        == checkpoint
                    )
                    plan = client.agent_workspace_restore(identifier, metadata["plan_id"])
                    assert (
                        client.agent_workspace_preview_restore(
                            identifier, CheckpointRestorePreview.model_validate(metadata["preview"])
                        )
                        == plan
                    )
                    assert (
                        client.agent_workspace_restore_by_key(
                            identifier, metadata["preview"]["request_key"]
                        )
                        == plan
                    )
                    assert len(client.agent_workspace_checkpoints(identifier)) == 2
                    assert len(client.agent_workspace_restores(identifier)) == 1
                    if mode == "repeat":
                        assert plan.status == "done"
                        (root / "file.txt").write_text("C")
                    else:
                        assert plan.phase == "ready"
                    client.agent_workspace_execute_restore(
                        identifier,
                        CheckpointRestoreExecute.model_validate(metadata["execute"]),
                        confirmed=True,
                    )
                    plan = client.agent_workspace_wait_restore(identifier, metadata["plan_id"])
                    assert plan.status == "done", plan
                    operation = plan.items[0].operation
                    assert operation is not None
                    if mode == "repeat":
                        assert operation.operation_id == metadata["operation_id"]
                        assert (root / "file.txt").read_text() == "C"
                    else:
                        assert (root / "file.txt").read_text() == "A"
                        metadata["operation_id"] = operation.operation_id
                        metadata_file.write_text(json.dumps(metadata))
                    assert (
                        len(client.agent_workspace_history(identifier, operation.proposal_id)) == 1
                    )
                for command, expected_field, expected_id in (
                    ("checkpoint", "checkpoint_id", checkpoint.checkpoint_id),
                    ("restore", "plan_id", plan.plan_id),
                ):
                    cli = subprocess.run(
                        [
                            sys.executable,
                            "-I",
                            "-m",
                            "krim_agent",
                            "workspace",
                            command,
                            identifier,
                            expected_id,
                            "--url",
                            url,
                        ],
                        capture_output=True,
                        text=True,
                        timeout=20,
                    )
                    assert cli.returncode == 0, cli.stderr
                    assert json.loads(cli.stdout)[expected_field] == expected_id
                return {
                    "mode": mode,
                    "package": krim_agent.__file__,
                    "agent_id": identifier,
                    "checkpoint_id": checkpoint.checkpoint_id,
                    "plan_id": plan.plan_id,
                    "status": plan.status,
                    "cli_verified": True,
                }

        result = await asyncio.to_thread(check)
        assert not await app.state.runtime.repository.active_runs()
        print(json.dumps(result), flush=True)
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 30)
        listener.close()


asyncio.run(main())
