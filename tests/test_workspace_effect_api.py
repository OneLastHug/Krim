"""真实HTTP、同步Python客户端和CLI查询同一冻结保留证明。"""

import asyncio
import hashlib
import json
import socket
import sys

import httpx
import pytest
import uvicorn
from test_workspace_management_api import allow_or_ask, cli

from krim_agent.api import create_app
from krim_agent.client import KrimClient
from krim_agent.workspace_change import (
    FileChangeSpec,
    WorkspaceChangeAction,
    WorkspacePreviewRequest,
)

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="需要Linux文件事务")


async def test_proof_across_real_tcp_sdk_cli_and_scope(runtime, tmp_path):
    file = tmp_path / "note.txt"
    file.write_text("A\n")
    runtime.tool_policy = allow_or_ask
    app = create_app(runtime=runtime, workspace_root=tmp_path, workspace_writable=True)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    url = f"http://127.0.0.1:{listener.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(10):
            while not server.started:
                if task.done():
                    await task
                await asyncio.sleep(0.01)

        def flow():
            with KrimClient(url) as client:
                agent = client.agent_spawn("default")
                proposals = []
                for index, content in enumerate(("B\n", "C\n")):
                    current = client.agent_read_file(agent.agent_id, "note.txt")
                    proposed = client.agent_workspace_preview(
                        agent.agent_id,
                        WorkspacePreviewRequest(
                            spec=FileChangeSpec(
                                path="note.txt",
                                operation="write",
                                expected_sha256=current.sha256,
                                content=content,
                            ),
                            request_key=f"preview-{index}",
                        ),
                    )
                    applied = client.agent_workspace_change(
                        agent.agent_id,
                        WorkspaceChangeAction(
                            proposal_id=proposed.proposal_id,
                            action="apply",
                            expected_revision=proposed.revision,
                            request_key=f"apply-{index}",
                        ),
                        confirmed=True,
                    )
                    assert applied.status == "done"
                    proposals.append(
                        client.agent_workspace_proposal(agent.agent_id, proposed.proposal_id)
                    )
                kept = client.agent_workspace_change(
                    agent.agent_id,
                    WorkspaceChangeAction(
                        proposal_id=proposals[0].proposal_id,
                        action="keep",
                        expected_revision=proposals[0].revision,
                        request_key="keep-first",
                    ),
                    confirmed=True,
                )
                assert kept.status == "done" and not kept.receipt.changed
                assert kept.receipt.after_sha256 == hashlib.sha256(b"B\n").hexdigest()
                proof = client.agent_workspace_operation_proof(agent.agent_id, kept.operation_id)
                assert proof.kind == "successor_chain"
                assert proof.tip_proposal_id == proposals[1].proposal_id
                assert proof.tip_after_sha256 == hashlib.sha256(b"C\n").hexdigest()
                other = client.agent_spawn("default")
                return (
                    agent.agent_id,
                    other.agent_id,
                    kept.operation_id,
                    proof.model_dump(mode="json"),
                )

        agent_id, other_id, operation_id, proof = await asyncio.to_thread(flow)
        code, stdout, stderr = await cli(url, "proof", agent_id, operation_id)
        assert code == 0, stderr
        assert json.loads(stdout) == proof
        async with httpx.AsyncClient(base_url=url) as client:
            response = await client.get(
                f"/v1/agents/{agent_id}/workspace/operations/{operation_id}/proof"
            )
            assert response.status_code == 200 and response.json() == proof
            denied = await client.get(
                f"/v1/agents/{other_id}/workspace/operations/{operation_id}/proof"
            )
            assert denied.status_code == 404
            assert operation_id not in denied.text
            malformed = await client.get(
                f"/v1/agents/{agent_id}/workspace/operations/bad%20identifier/proof"
            )
            assert malformed.status_code == 422
        assert file.read_text() == "C\n"
        assert str(tmp_path) not in json.dumps(proof)
        assert "content" not in proof and "witness" not in proof
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 15)
        listener.close()
