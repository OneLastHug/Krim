"""POST实际已接收但HTTP回程丢失，Client只读原键核实而不重发。"""

import asyncio

import httpx
import pytest
from test_directory_restore_control import checkpoint, drain, execute_request, setup
from test_directory_restore_public_consumers import cli
from test_tree_move_public_consumers import api_for, tcp_for

from krim_agent.client import KrimClient
from krim_agent.sdk import DirectoryRestorePreview, DirectoryRestoreTarget
from krim_agent.workspace_directory_restore_writer import WorkspaceDirectoryRestoreWriter


async def test_post_reply_lost_after_real_server_acceptance_recovers_same_operation_without_retry(
    runtime, tmp_path, monkeypatch
):
    service, agent, binding = await setup(runtime, tmp_path)
    try:
        target = await checkpoint(agent)
        (binding.workspace.root / "tree/nested/nul").write_bytes(b"current\x80\0")
        request = DirectoryRestorePreview(
            target=DirectoryRestoreTarget(identifier=target.checkpoint_id), request_key="preview"
        )
        planned = await agent.preview_directory_checkpoint_restore(request)
        planned = await agent.wait_directory_checkpoint_restore(planned.plan_id, timeout_seconds=10)
        await drain(service)
        assert planned.phase == "ready", planned
        wanted = execute_request(planned, "original-lost-reply")
        calls = []

        class LostReply(httpx.BaseTransport):
            def __init__(self):
                self.original = httpx.HTTPTransport()

            def handle_request(self, request):
                calls.append((request.method, request.url.path))
                response = self.original.handle_request(request)
                if request.method == "POST" and request.url.path.endswith("/start"):
                    response.read()
                    response.close()
                    raise httpx.ReadError("服务已接收原POST，仅回程未知", request=request)
                return response

            def close(self):
                self.original.close()

        async with api_for(runtime, service, monkeypatch) as (app, _api), tcp_for(app) as url:

            def lost_start():
                with KrimClient(url) as client:
                    client._http.close()
                    client._http = httpx.Client(base_url=url, transport=LostReply())
                    with pytest.raises(httpx.ReadError):
                        client.agent_workspace_execute_directory_restore(agent.agent_id, wanted)
                    operation = client.agent_workspace_directory_restore_operation_by_key(
                        agent.agent_id, wanted.request_key
                    )
                    assert operation is not None and operation.plan_id == planned.plan_id
                    return operation.operation_id

            operation_id = await asyncio.to_thread(lost_start)
            result = await agent.wait_directory_checkpoint_restore(
                planned.plan_id, timeout_seconds=10
            )
            await drain(service)
            assert result.status == "done" and result.operation_id == operation_id, result
            assert sum(method == "POST" for method, _ in calls) == 1
            assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"

            def forbidden(*_args, **_kwargs):
                pytest.fail("丢回程核实不能再次发布")

            monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "prepare", forbidden)
            monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "apply", forbidden)
            assert await cli(url, agent.agent_id, "get", planned.plan_id) == result.model_dump(
                mode="json"
            )
            operation = await cli(url, agent.agent_id, "operation-by-key", wanted.request_key)
            assert operation["operation_id"] == operation_id and operation["status"] == "done"
    finally:
        await service.close()
