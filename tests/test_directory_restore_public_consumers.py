"""SDK/REST/真实TCP Client和独立CLI消费同一完整目录恢复事实。"""

import asyncio
import json
import sys

import pytest
from test_directory_restore_control import checkpoint, drain, execute_request, setup
from test_tree_move_public_consumers import api_for, tcp_for

from krim_agent.client import KrimClient
from krim_agent.sdk import DirectoryRestorePreview, DirectoryRestoreTarget
from krim_agent.workspace_directory_restore_writer import WorkspaceDirectoryRestoreWriter
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


async def cli(url, agent_id, action, value=None, *options, expected=0):
    arguments = [sys.executable, "-I", "-m", "krim_agent", "directory-restores", action, agent_id]
    if value is not None:
        arguments.append(value)
    process = await asyncio.create_subprocess_exec(
        *arguments,
        "--url",
        url,
        *options,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        output, errors = await asyncio.wait_for(process.communicate(), 15)
        assert process.returncode == expected, errors.decode()
        return json.loads(output)
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()


async def test_complete_sdk_http_tcp_cli_backup_rollback_and_cold_original_keys(
    runtime, tmp_path, monkeypatch
):
    service, agent, binding = await setup(runtime, tmp_path)
    try:
        target = await checkpoint(agent)
        (binding.workspace.root / "tree/nested/nul").write_bytes(b"current\x80\0")
        wanted = DirectoryRestorePreview(
            target=DirectoryRestoreTarget(identifier=target.checkpoint_id),
            request_key="原键/../?confirmed=true#%&",
        )
        planned = await agent.preview_directory_checkpoint_restore(wanted)
        planned = await agent.wait_directory_checkpoint_restore(planned.plan_id, timeout_seconds=10)
        assert planned.phase == "ready", planned
        await drain(service)
        page = await agent.directory_checkpoint_restore_diff(planned.plan_id)
        assert page.plan_id == planned.plan_id and page.scope_id == planned.scope_id
        assert len(page.entries) == 1 and page.entries[0].path == "tree/nested/nul"
        async with api_for(runtime, service, monkeypatch) as (app, api):
            base = f"/v1/agents/{agent.agent_id}/workspace/directory-restores"
            response = await api.post(
                base + "/start", json=execute_request(planned).model_dump(mode="json")
            )
            assert response.status_code == 202, response.text
            finished = await agent.wait_directory_checkpoint_restore(
                planned.plan_id, timeout_seconds=10
            )
            assert finished.status == "done", finished
            await drain(service)
            assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"
            assert (await api.get(base + f"/{planned.plan_id}/diff")).json() == page.model_dump(
                mode="json"
            )
            # 完整备份以原plan引用，实际第二次整树发布，不能仅展示备份存在。
            fallback = await agent.preview_directory_checkpoint_restore(
                DirectoryRestorePreview(
                    target=DirectoryRestoreTarget(kind="baseline", identifier=planned.plan_id),
                    request_key="rollback",
                )
            )
            fallback = await agent.wait_directory_checkpoint_restore(
                fallback.plan_id, timeout_seconds=10
            )
            await drain(service)
            assert fallback.phase == "ready", fallback
            await agent.execute_directory_checkpoint_restore(
                execute_request(fallback, "rollback-execute")
            )
            rolled = await agent.wait_directory_checkpoint_restore(
                fallback.plan_id, timeout_seconds=10
            )
            await drain(service)
            assert rolled.status == "done", rolled
            assert (binding.workspace.root / "tree/nested/nul").read_bytes() == b"current\x80\0"

            def forbidden(*_args, **_kwargs):
                pytest.fail("公开历史/原键不得重新捕获、准备或发布")

            monkeypatch.setattr(WorkspaceTreeSnapshotReader, "capture", forbidden)
            monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "prepare", forbidden)
            monkeypatch.setattr(WorkspaceDirectoryRestoreWriter, "apply", forbidden)
            value = finished.model_dump(mode="json")
            operation = await agent.directory_checkpoint_restore_operation(finished.operation_id)
            assert (
                await api.get(base + "/by-key", params={"request_key": wanted.request_key})
            ).json() == value
            async with tcp_for(app) as url:

                def client_calls():
                    with KrimClient(url) as client:
                        assert (
                            client.agent_workspace_directory_restore(
                                agent.agent_id, planned.plan_id
                            )
                            == finished
                        )
                        assert (
                            client.agent_workspace_directory_restore_by_key(
                                agent.agent_id, wanted.request_key
                            )
                            == finished
                        )
                        assert (
                            client.agent_workspace_directory_restore_diff(
                                agent.agent_id, planned.plan_id
                            )
                            == page
                        )
                        assert (
                            client.agent_workspace_directory_restore_operation(
                                agent.agent_id, finished.operation_id
                            )
                            == operation
                        )
                        assert (
                            client.agent_workspace_directory_restore_operation_by_key(
                                agent.agent_id, "execute"
                            )
                            == operation
                        )
                        assert (
                            client.agent_workspace_preview_directory_restore(agent.agent_id, wanted)
                            == finished
                        )
                        assert (
                            client.agent_workspace_execute_directory_restore(
                                agent.agent_id, execute_request(planned)
                            )
                            == finished
                        )
                        assert (
                            client.agent_workspace_verify_directory_restore(
                                agent.agent_id, finished.operation_id
                            )
                            == finished
                        )
                        assert (
                            client.agent_workspace_wait_directory_restore(
                                agent.agent_id, planned.plan_id, timeout_seconds=0
                            )
                            == finished
                        )
                        assert (
                            client.agent_workspace_cancel_directory_restore(
                                agent.agent_id, planned.plan_id
                            )
                            == finished
                        )
                        assert len(client.agent_workspace_directory_restores(agent.agent_id)) == 2
                        assert (
                            client.agent_workspace_directory_restore_by_key(
                                agent.agent_id, "missing"
                            )
                            is None
                        )
                        assert (
                            client.agent_workspace_directory_restore_operation_by_key(
                                agent.agent_id, "missing"
                            )
                            is None
                        )

                await asyncio.to_thread(client_calls)
                assert await cli(url, agent.agent_id, "get", planned.plan_id) == value
                assert await cli(url, agent.agent_id, "by-key", wanted.request_key) == value
                assert await cli(url, agent.agent_id, "diff", planned.plan_id) == page.model_dump(
                    mode="json"
                )
                assert await cli(
                    url, agent.agent_id, "operation", finished.operation_id
                ) == operation.model_dump(mode="json")
                assert await cli(
                    url, agent.agent_id, "operation-by-key", "execute"
                ) == operation.model_dump(mode="json")
                assert len(await cli(url, agent.agent_id, "list")) == 2
                assert (
                    await cli(
                        url, agent.agent_id, "wait", planned.plan_id, "--timeout-seconds", "0"
                    )
                    == value
                )
                assert await cli(url, agent.agent_id, "cancel", planned.plan_id) == value
                assert await cli(url, agent.agent_id, "verify", finished.operation_id) == value
                request_file = tmp_path / "preview-request.json"
                request_file.write_text(wanted.model_dump_json())
                assert (
                    await cli(url, agent.agent_id, "preview", None, "--request", str(request_file))
                    == value
                )
                request_file.write_text(execute_request(planned).model_dump_json())
                assert (
                    await cli(url, agent.agent_id, "start", None, "--request", str(request_file))
                    == value
                )
    finally:
        await service.close()


async def test_rest_private_duplicate_nonempty_body_and_cross_agent_refusal(
    runtime, tmp_path, monkeypatch
):
    service, agent, binding = await setup(runtime, tmp_path)
    try:
        target = await checkpoint(agent)
        wanted = DirectoryRestorePreview(
            target=DirectoryRestoreTarget(identifier=target.checkpoint_id), request_key="preview"
        )
        planned = await agent.preview_directory_checkpoint_restore(wanted)
        planned = await agent.wait_directory_checkpoint_restore(planned.plan_id, timeout_seconds=10)
        await drain(service)
        assert planned.phase == "ready", planned
        async with api_for(runtime, service, monkeypatch) as (_app, api):
            base = f"/v1/agents/{agent.agent_id}/workspace/directory-restores"
            for private in (
                "token",
                "material_id",
                "root_path",
                "prepared",
                "model_source",
                "owner_id",
            ):
                rejected = await api.post(
                    base + "/preview", json={**wanted.model_dump(mode="json"), private: "invalid"}
                )
                assert rejected.status_code == 422, rejected.text
                rejected = await api.post(
                    base + "/start",
                    json={**execute_request(planned).model_dump(mode="json"), private: "invalid"},
                )
                assert rejected.status_code == 422, rejected.text
            for method, path, options in (
                ("GET", base, {"params": {"limit": 33}}),
                ("GET", base, {"params": {"offset": -1}}),
                ("GET", base, {"params": {"confirmed": "true"}}),
                (
                    "GET",
                    base + "/by-key",
                    {"params": [("request_key", "preview"), ("request_key", "other")]},
                ),
                ("GET", base + f"/{planned.plan_id}/diff", {"params": {"limit": 129}}),
                ("GET", base + f"/{planned.plan_id}", {"json": {"token": "private"}}),
                ("POST", base + f"/{planned.plan_id}/cancel", {"json": {"token": "private"}}),
                ("POST", base + f"/{planned.plan_id}/wait", {"params": {"timeout_seconds": "nan"}}),
                (
                    "POST",
                    base + "/start",
                    {
                        "params": [("confirmed", "true"), ("confirmed", "false")],
                        "json": execute_request(planned).model_dump(mode="json"),
                    },
                ),
            ):
                response = await api.request(method, path, **options)
                assert response.status_code == 422, response.text
            other = await service.spawn("directory-restore")
            response = await api.get(
                f"/v1/agents/{other.agent_id}/workspace/directory-restores/{planned.plan_id}"
            )
            assert response.status_code == 404
            response = await api.post(
                f"/v1/agents/{other.agent_id}/workspace/directory-restores/preview",
                json=wanted.model_dump(mode="json"),
            )
            assert response.status_code in {403, 404}
        assert (await agent.directory_checkpoint_restore(planned.plan_id)).phase == "ready"
        assert not list(binding.workspace.root.glob("._krim-tx-*"))
    finally:
        await service.close()
