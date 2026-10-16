"""CLI在网络动作前拒绝误用/私有请求，并明确显示未知回执。"""

import asyncio
import sys

import pytest
from test_missing_directory_restore_control import drain, execute_request, ready, setup
from test_missing_directory_restore_public_consumers import cli
from test_tree_move_public_consumers import api_for, tcp_for

import krim_agent.workspace_missing_directory_writer as physical
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter


@pytest.mark.parametrize(
    "fault",
    [
        "get_request",
        "list_identifier",
        "start_without_file",
        "verify_file",
        "private",
        "oversize",
        "serve_workspace",
    ],
)
async def test_cli_invalid_actions_are_rejected_before_network(tmp_path, fault):
    request = tmp_path / "request.json"
    request.write_text(
        '{"target":{"identifier":"target"},"request_key":"preview","token":"private"}'
    )
    if fault == "oversize":
        request.write_bytes(b" " * 65537)
    commands = {
        "get_request": ["get", "agent", "plan", "--request", str(request)],
        "list_identifier": ["list", "agent", "plan"],
        "start_without_file": ["start", "agent", "--confirm"],
        "verify_file": ["verify", "agent", "operation", "--request", str(request)],
        "private": ["preview", "agent", "--request", str(request)],
        "oversize": ["preview", "agent", "--request", str(request)],
    }
    args = (
        ["serve", "--missing-directory-restores"]
        if fault == "serve_workspace"
        else ["missing-directory-restores", *commands[fault], "--url", "http://127.0.0.1:1"]
    )
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        "-m",
        "krim_agent",
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        output, errors = await asyncio.wait_for(process.communicate(), 15)
        assert process.returncode == 2 and not output, errors.decode()
        assert b"Traceback" not in errors
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()


async def test_cli_unknown_exit_and_rest_verify_keep_same_original_operation(
    runtime, tmp_path, monkeypatch
):
    service, agent, binding = await setup(runtime, tmp_path)
    try:
        _, planned = await ready(service, agent, binding)
        rename = physical.rename_at

        def lost(*args, **kwargs):
            rename(*args, **kwargs)
            raise OSError("原namespace之后仅回程未知")

        monkeypatch.setattr(physical, "rename_at", lost)
        await agent.execute_missing_directory_checkpoint_restore(execute_request(planned))
        await drain(service)
        unknown = await agent.missing_directory_checkpoint_restore(planned.plan_id)
        assert unknown.status == "uncertain"

        def forbidden(*_args, **_kwargs):
            pytest.fail("未知操作核实不能重放Writer或namespace")

        monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "prepare", forbidden)
        monkeypatch.setattr(WorkspaceMissingDirectoryRestoreWriter, "apply", forbidden)
        monkeypatch.setattr(physical, "rename_at", forbidden)
        async with api_for(runtime, service, monkeypatch) as (app, api), tcp_for(app) as url:
            read = await cli(
                url, agent.agent_id, "wait", planned.plan_id, "--timeout-seconds", "0", expected=1
            )
            assert read["status"] == "uncertain" and read["operation_id"] == unknown.operation_id
            cancel = await cli(url, agent.agent_id, "cancel", planned.plan_id, expected=1)
            assert cancel["status"] == "uncertain" and cancel["cancel_requested"]
            response = await api.post(
                f"/v1/agents/{agent.agent_id}/workspace/missing-directory-restores/operations/{unknown.operation_id}/verify"
            )
            assert response.status_code == 202, response.text
            await drain(service)
            finished = await agent.missing_directory_checkpoint_restore(planned.plan_id)
            assert finished.status == "done" and finished.operation_id == unknown.operation_id
            assert (
                finished.cancel_requested
                and (binding.workspace.root / "tree/nested/nul").read_bytes() == b"\0\xff"
            )
            assert await cli(url, agent.agent_id, "get", planned.plan_id) == finished.model_dump(
                mode="json"
            )
    finally:
        await service.close()
