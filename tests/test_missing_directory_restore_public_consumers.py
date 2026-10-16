"""SDK/真实HTTP-TCP Client/独立CLI读取同一缺失baseline和原唯一操作。"""

import asyncio
import json
import sys

import pytest
from test_missing_directory_restore_control import checkpoint, drain, execute_request, setup
from test_tree_move_public_consumers import api_for, tcp_for

from krim_agent.client import KrimClient
from krim_agent.sdk import DirectoryRestoreTarget, MissingDirectoryRestorePreview
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter


async def cli(url, agent_id, action, value=None, *options, expected=0):
    args = [
        sys.executable,
        "-I",
        "-m",
        "krim_agent",
        "missing-directory-restores",
        action,
        agent_id,
    ]
    if value is not None:
        args.append(value)
    process = await asyncio.create_subprocess_exec(
        *args,
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


async def test_missing_sdk_http_tcp_cli_complete_fact_and_cold_original_requests(
    runtime, tmp_path, monkeypatch
):
    service, agent, binding = await setup(runtime, tmp_path)
    try:
        captured = await checkpoint(agent)
        (tmp_path / "tree").rename(tmp_path / "retained-original")
        wanted = MissingDirectoryRestorePreview(
            target=DirectoryRestoreTarget(identifier=captured.checkpoint_id),
            request_key="原键/../?confirmed=true#%&",
        )
        planned = await agent.preview_missing_directory_checkpoint_restore(wanted)
        planned = await agent.wait_missing_directory_checkpoint_restore(
            planned.plan_id, timeout_seconds=10
        )
        await drain(service)
        assert planned.phase == "ready" and planned.baseline.kind == "absent", planned
        page = await agent.missing_directory_checkpoint_restore_diff(planned.plan_id)
        assert page.total == planned.target_snapshot.entry_count == len(page.entries)
        assert page.next_offset is None and all(
            item.before is None and item.operation == "create" for item in page.entries
        )
        async with api_for(runtime, service, monkeypatch) as (app, api):
            base = f"/v1/agents/{agent.agent_id}/workspace/missing-directory-restores"
            response = await api.post(
                base + "/start", json=execute_request(planned).model_dump(mode="json")
            )
            assert response.status_code == 202, response.text
            final = await agent.wait_missing_directory_checkpoint_restore(
                planned.plan_id, timeout_seconds=10
            )
            await drain(service)
            assert (
                final.phase == "finished"
                and (tmp_path / "tree/nested/nul").read_bytes() == b"\0\xff"
            ), final
            operation = await agent.missing_directory_checkpoint_restore_operation(
                final.operation_id
            )
            assert (await api.get(base + f"/{planned.plan_id}/diff")).json() == page.model_dump(
                mode="json"
            )

            def forbidden(*_args, **_kwargs):
                pytest.fail("原键/历史公开消费不能重新取得观察或准备发布")

            monkeypatch.setattr(WorkspaceDirectoryAbsenceReader, "capture", forbidden)
            monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "prepare", forbidden)
            monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "apply", forbidden)
            async with tcp_for(app) as url:

                def client_calls():
                    with KrimClient(url) as client:
                        assert (
                            client.agent_workspace_missing_directory_restore(
                                agent.agent_id, planned.plan_id
                            )
                            == final
                        )
                        assert (
                            client.agent_workspace_missing_directory_restore_by_key(
                                agent.agent_id, wanted.request_key
                            )
                            == final
                        )
                        assert (
                            client.agent_workspace_missing_directory_restore_diff(
                                agent.agent_id, planned.plan_id
                            )
                            == page
                        )
                        assert (
                            client.agent_workspace_missing_directory_restore_operation(
                                agent.agent_id, final.operation_id
                            )
                            == operation
                        )
                        assert (
                            client.agent_workspace_missing_directory_restore_operation_by_key(
                                agent.agent_id, "execute"
                            )
                            == operation
                        )
                        assert client.agent_workspace_missing_directory_restores(
                            agent.agent_id
                        ) == [final]
                        assert (
                            client.agent_workspace_preview_missing_directory_restore(
                                agent.agent_id, wanted
                            )
                            == final
                        )
                        assert (
                            client.agent_workspace_execute_missing_directory_restore(
                                agent.agent_id, execute_request(planned)
                            )
                            == final
                        )
                        assert (
                            client.agent_workspace_verify_missing_directory_restore(
                                agent.agent_id, final.operation_id
                            )
                            == final
                        )
                        assert (
                            client.agent_workspace_wait_missing_directory_restore(
                                agent.agent_id, planned.plan_id, timeout_seconds=0
                            )
                            == final
                        )
                        assert (
                            client.agent_workspace_cancel_missing_directory_restore(
                                agent.agent_id, planned.plan_id
                            )
                            == final
                        )
                        assert (
                            client.agent_workspace_missing_directory_restore_by_key(
                                agent.agent_id, "missing"
                            )
                            is None
                        )
                        assert (
                            client.agent_workspace_missing_directory_restore_operation_by_key(
                                agent.agent_id, "missing"
                            )
                            is None
                        )

                await asyncio.to_thread(client_calls)
                expected = final.model_dump(mode="json")
                for action, identifier in (
                    ("get", planned.plan_id),
                    ("by-key", wanted.request_key),
                    ("wait", planned.plan_id),
                    ("cancel", planned.plan_id),
                    ("verify", final.operation_id),
                ):
                    assert await cli(url, agent.agent_id, action, identifier) == expected
                assert await cli(url, agent.agent_id, "list") == [expected]
                assert await cli(url, agent.agent_id, "diff", planned.plan_id) == page.model_dump(
                    mode="json"
                )
                assert await cli(
                    url, agent.agent_id, "operation", final.operation_id
                ) == operation.model_dump(mode="json")
                assert await cli(
                    url, agent.agent_id, "operation-by-key", "execute"
                ) == operation.model_dump(mode="json")
                request_file = tmp_path / "original-request.json"
                for action, request in (("preview", wanted), ("start", execute_request(planned))):
                    request_file.write_text(request.model_dump_json())
                    assert (
                        await cli(url, agent.agent_id, action, None, "--request", str(request_file))
                        == expected
                    )
            assert not service.missing_directory_restores._tasks
            assert binding.workspace.root == tmp_path
    finally:
        await service.close()


async def test_missing_rest_refuses_private_query_body_and_foreign_agent(
    runtime, tmp_path, monkeypatch
):
    (
        service,
        agent,
        _,
    ) = await setup(runtime, tmp_path)
    try:
        captured = await checkpoint(agent)
        (tmp_path / "tree").rename(tmp_path / "retained-original")
        wanted = MissingDirectoryRestorePreview(
            target=DirectoryRestoreTarget(identifier=captured.checkpoint_id), request_key="original"
        )
        planned = await agent.preview_missing_directory_checkpoint_restore(wanted)
        planned = await agent.wait_missing_directory_checkpoint_restore(
            planned.plan_id, timeout_seconds=10
        )
        await drain(service)
        async with api_for(runtime, service, monkeypatch) as (_app, api):
            base = f"/v1/agents/{agent.agent_id}/workspace/missing-directory-restores"
            for private in (
                "token",
                "material_id",
                "root_path",
                "model_source",
                "owner_id",
                "baseline",
            ):
                rejected = await api.post(
                    base + "/preview", json={**wanted.model_dump(mode="json"), private: "forged"}
                )
                assert rejected.status_code == 422, (private, rejected.text)
            for suffix in ("?limit=1&limit=2", "?extra=true", "?offset=true", "?limit=33"):
                rejected = await api.get(base + suffix)
                assert rejected.status_code == 422, (suffix, rejected.text)
            for method, suffix in (
                ("GET", f"/{planned.plan_id}"),
                ("POST", f"/{planned.plan_id}/wait"),
                ("POST", f"/{planned.plan_id}/cancel"),
            ):
                rejected = await api.request(method, base + suffix, content=b"{}")
                assert rejected.status_code == 422, (method, suffix, rejected.text)
            other = await service.spawn("missing-directory-restore")
            foreign = await api.get(
                f"/v1/agents/{other.agent_id}/workspace/missing-directory-restores/{planned.plan_id}"
            )
            assert foreign.status_code == 404, foreign.text
            assert await agent.missing_directory_checkpoint_restore(planned.plan_id) == planned
            assert not (tmp_path / "tree").exists()
    finally:
        await service.close()
