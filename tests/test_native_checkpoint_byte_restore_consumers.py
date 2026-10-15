"""SDK/HTTP/TCP/真实CLI的同字节恢复与严格Client回程。"""

import asyncio
import copy
import json
import sys
from pathlib import Path

import httpx
import pytest
from test_native_checkpoint_byte_capture import capture
from test_native_checkpoint_byte_restore import mixed_setup, restore_execute, restore_preview
from test_tree_move_public_consumers import tcp_for

import krim_agent.api.server as server_module
from krim_agent.client import ClientError, KrimClient
from krim_agent.sdk import CheckpointRestorePreview
from krim_agent.workspace_writer import WorkspaceWriter


async def test_sdk_rest_tcp_client_cli_same_byte_restore_backup_and_cold_keys(
    runtime, tmp_path, monkeypatch
):
    file = tmp_path / "file"
    file.write_bytes(b"\0\xfftarget")
    service, agent, _ = await mixed_setup(runtime, tmp_path)
    monkeypatch.setattr(server_module, "AgentService", lambda *_args, **_kwargs: service)
    app = server_module.create_app(runtime=runtime)
    base = f"/v1/agents/{agent.agent_id}/workspace"
    count = 0
    original = WorkspaceWriter.capture_file

    def counted(*args, **kwargs):
        nonlocal count
        count += 1
        return original(*args, **kwargs)

    async def cli(url, command, *args):
        child = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            "-m",
            "krim_agent",
            "workspace",
            command,
            agent.agent_id,
            *args,
            "--url",
            url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await asyncio.wait_for(child.communicate(), 60)
        assert child.returncode == 0, err.decode()
        return json.loads(out)

    try:
        _, checkpoint = await capture(agent, ["file"])
        file.write_bytes(b"before\xff\0")
        monkeypatch.setattr(WorkspaceWriter, "capture_file", counted)
        request = CheckpointRestorePreview(
            checkpoint_id=checkpoint.checkpoint_id,
            request_key="consumer-preview",
            baseline_capture_format="bytes",
        )
        async with tcp_for(app) as url:
            with KrimClient(url) as client:
                accepted = await asyncio.to_thread(
                    client.agent_workspace_preview_restore, agent.agent_id, request
                )
                plan = await asyncio.to_thread(
                    client.agent_workspace_wait_restore, agent.agent_id, accepted.plan_id
                )
                assert plan.phase == "ready" and plan.baseline_capture_format == "bytes", plan
                assert count == 1
                path = tmp_path / "preview.json"
                path.write_text(request.model_dump_json())
                assert await cli(url, "preview-restore", "--request", str(path)) == plan.model_dump(
                    mode="json"
                )
                assert await agent.preview_file_checkpoint_restore(request) == plan
                assert count == 1
                done = await cli(
                    url,
                    "execute-restore",
                    plan.plan_id,
                    "--revision",
                    str(plan.revision),
                    "--request-key",
                    "consumer-execute",
                    "--confirm",
                )
                # CLI接受后台任务后，再读原计划等待实际状态；不是再次execute。
                final = await asyncio.to_thread(
                    client.agent_workspace_wait_restore, agent.agent_id, plan.plan_id
                )
                assert final.status == "done" and file.read_bytes() == b"\0\xfftarget", (
                    done,
                    final,
                )
                assert (
                    await asyncio.to_thread(
                        client.agent_workspace_restore_by_key, agent.agent_id, request.request_key
                    )
                    == final
                )
                assert (await asyncio.to_thread(client.agent_workspace_restores, agent.agent_id))[
                    0
                ] == final
                assert await cli(
                    url, "restore-by-key", "--request-key", request.request_key
                ) == final.model_dump(mode="json")
                assert count == 1
                backup = await asyncio.to_thread(
                    client.agent_workspace_checkpoint, agent.agent_id, plan.baseline_checkpoint_id
                )
                assert backup.capture_format == "bytes" and backup.files[0].content_kind == "binary"
            async with httpx.AsyncClient(base_url=url) as api:
                response = await api.get(
                    base + "/native-restores/by-key", params={"request_key": request.request_key}
                )
                assert response.status_code == 200 and response.json() == final.model_dump(
                    mode="json"
                )
                invalid = await api.post(
                    base + "/native-restores/preview",
                    json={**request.model_dump(mode="json"), "baseline_capture_format": "binary"},
                )
                assert invalid.status_code == 422
            rollback = await restore_preview(agent, backup, key="consumer-rollback")
            restored = await restore_execute(agent, rollback, key="consumer-rollback-execute")
            assert restored.status == "done" and file.read_bytes() == b"before\xff\0", restored
    finally:
        await service.close()


@pytest.mark.parametrize(
    "fault",
    [
        "scope",
        "target",
        "key",
        "format",
        "id",
        "by-key",
        "execute-key",
        "execute-revision",
        "position",
    ],
)
def test_client_rejects_schema_valid_wrong_restore_facts(fault):
    value = copy.deepcopy(
        json.loads(
            (Path(__file__).parent / "fixtures/native_restore_legacy_vectors.json").read_text()
        )["view"]
    )
    value["scope_id"] = "session:agent"
    value["baseline_capture_format"] = "bytes"
    value["plan_id"] = "original-plan"
    if fault == "scope":
        value["scope_id"] = "session:another"
    elif fault == "target":
        value["checkpoint_id"] = "another-target"
    elif fault in {"key", "by-key"}:
        value["request_key"] = "another-key"
    elif fault == "format":
        value.pop("baseline_capture_format")
    elif fault == "id":
        value["plan_id"] = "another-plan"
    elif fault == "execute-key":
        value["execute_key"] = "another-key"
    elif fault == "execute-revision":
        value["execute_key"] = "original-execute"
    elif fault == "position":
        value["items"][0]["position"] = 1
    with KrimClient(
        "http://127.0.0.1",
        transport=httpx.MockTransport(lambda _request: httpx.Response(200, json=value)),
    ) as client:
        with pytest.raises(ClientError) as rejected:
            if fault == "id":
                client.agent_workspace_restore("agent", "original-plan")
            elif fault == "by-key":
                client.agent_workspace_restore_by_key("agent", "original-preview")
            elif fault in {"execute-key", "execute-revision"}:
                from krim_agent.sdk import CheckpointRestoreExecute

                client.agent_workspace_execute_restore(
                    "agent",
                    CheckpointRestoreExecute(
                        plan_id="original-plan", expected_revision=1, request_key="original-execute"
                    ),
                )
            else:
                client.agent_workspace_preview_restore(
                    "agent",
                    CheckpointRestorePreview(
                        checkpoint_id="original-target",
                        request_key="original-preview",
                        baseline_capture_format="bytes",
                    ),
                )
        assert rejected.value.code == "workspace_restore_response" and rejected.value.status == 502
