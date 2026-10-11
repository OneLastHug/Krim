import asyncio
import base64
import json
import os
import shutil
import socket
import subprocess
import sys

import httpx
import pytest
import uvicorn
from test_command_management_api import NAMES

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.api import create_app
from krim_agent.client import ClientError, KrimClient
from krim_agent.config import Settings
from krim_agent.execution import CommandSpec, LinuxSandboxRunner
from krim_agent.sdk import AgentCommandRequest, CommandProposalRequest, WorkspaceChangeAction

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要实际TCP与命令执行",
)


@pytest.fixture
async def command_server(tmp_path):
    root = tmp_path / "files"
    root.mkdir()
    (root / "input.txt").write_text("input")
    app = create_app(
        Settings(
            database_url=f"sqlite+aiosqlite:///{tmp_path / 'tcp.sqlite'}",
            api_key="test-key",
            interactive_tools=True,
        ),
        workspace_root=root,
        workspace_writable=True,
        execution_runner=LinuxSandboxRunner(),
        execution_owned=True,
    )
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(10):
            while not server.started:
                if task.done():
                    await task
                await asyncio.sleep(0.01)
        yield f"http://127.0.0.1:{port}", root, app
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 20)
        listener.close()


def configure(client):
    client.agent_profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="commands",
                description="TCP client",
                workspace_key="local",
                execution_key="local",
                tool_names=NAMES,
                persistent=True,
            ),
        )
    )
    return client.agent_spawn("commands", persistent=True).agent_id


async def test_sync_client_uses_real_tcp_for_execution_and_publication(command_server):
    url, root, _app = command_server

    def scenario():
        with KrimClient(url, "test-key") as client:
            identifier = configure(client)
            request = AgentCommandRequest(
                request_key="tcp",
                input_paths=["input.txt"],
                command=CommandSpec(
                    argv=[
                        "/usr/bin/python3",
                        "-c",
                        "print(open('input.txt').read());open('output.txt','w').write('artifact')",
                    ]
                ),
            )
            accepted = client.agent_command_start(identifier, request, confirmed=True)
            result = client.agent_command_wait(identifier, accepted.command_id)
            assert result.outcome == "completed"
            assert (
                client.agent_command(identifier, result.command_id).command_id == result.command_id
            )
            assert client.agent_command_by_key(identifier, "tcp").command_id == result.command_id
            assert client.agent_command_history(identifier).entries[0].request_key == "tcp"
            assert (
                client.agent_command_inspect(identifier, result.command_id).request.command
                == request.command
            )
            assert (
                client.agent_command_read(
                    identifier, result.command_id, source="artifact", path="output.txt"
                ).text
                == "artifact"
            )
            changes = client.agent_command_changes(identifier, result.command_id)
            assert changes.entries[0].path == "output.txt"
            proposal = client.agent_command_propose(
                identifier,
                CommandProposalRequest(
                    command_id=result.command_id, path="output.txt", request_key="proposal"
                ),
            )
            operation = client.agent_workspace_change(
                identifier,
                WorkspaceChangeAction(
                    proposal_id=proposal.proposal.proposal_id,
                    action="apply",
                    expected_revision=0,
                    request_key="apply",
                ),
                confirmed=True,
            )
            assert operation.status == "done"
            assert (
                client.agent_command_start(identifier, request, confirmed=True).command_id
                == result.command_id
            )
            return identifier

    await asyncio.to_thread(scenario)
    assert (root / "output.txt").read_text() == "artifact"


async def test_sync_client_binary_input_and_exact_undo_use_real_tcp(command_server):
    url, root, _app = command_server
    target = root / "input.bin"
    original = b"\0\xffinput"
    target.write_bytes(original)

    def scenario():
        with KrimClient(url, "test-key") as client:
            identifier = configure(client)
            intent = AgentCommandRequest(
                request_key="binary-tcp",
                input_paths=["input.bin"],
                command=CommandSpec(
                    argv=[
                        "/usr/bin/python3",
                        "-I",
                        "-c",
                        "data=open('input.bin','rb').read();print(data.hex());"
                        "open('input.bin','wb').write(bytes([255,0,128]))",
                    ]
                ),
            )
            accepted = client.agent_command_start(identifier, intent, confirmed=True)
            result = client.agent_command_wait(identifier, accepted.command_id)
            assert result.outcome == "completed" and target.read_bytes() == original
            assert (
                client.agent_command_read(identifier, result.command_id).text
                == original.hex() + "\n"
            )
            raw = client.agent_command_read(
                identifier, result.command_id, source="artifact", path="input.bin"
            )
            assert base64.b64decode(raw.data_base64) == b"\xff\0\x80"
            proposal = client.agent_command_propose(
                identifier,
                CommandProposalRequest(
                    command_id=result.command_id, path="input.bin", request_key="binary-proposal"
                ),
            )
            assert proposal.proposal.preview.truncated
            for action in ("apply", "keep", "undo"):
                current = client.agent_workspace_proposal(identifier, proposal.proposal.proposal_id)
                receipt = client.agent_workspace_change(
                    identifier,
                    WorkspaceChangeAction(
                        proposal_id=current.proposal_id,
                        action=action,
                        expected_revision=current.revision,
                        request_key="binary-" + action,
                    ),
                    confirmed=True,
                )
                assert receipt.status == "done"
                assert target.read_bytes() == (original if action == "undo" else b"\xff\0\x80")
            assert (
                client.agent_command_start(identifier, intent, confirmed=True).command_id
                == result.command_id
            )
            assert len(client.agent_command_history(identifier).entries) == 1

    await asyncio.to_thread(scenario)
    assert target.read_bytes() == original


async def test_cli_commands_and_exit_codes_use_real_server(command_server, tmp_path):
    url, _root, _app = command_server

    def create():
        with KrimClient(url, "test-key") as client:
            return configure(client)

    identifier = await asyncio.to_thread(create)

    async def run(*args):
        return await asyncio.to_thread(
            subprocess.run,
            [sys.executable, "-m", "krim_agent", "commands", *args, "--url", url],
            capture_output=True,
            text=True,
            env={**os.environ, "KRIM_AGENT_API_KEY": "test-key"},
            timeout=30,
        )

    request = tmp_path / "request.json"
    request.write_text(
        json.dumps(
            {
                "request_key": "cli",
                "command": {"argv": ["/usr/bin/python3", "-c", "print('cli output')"]},
            }
        )
    )
    started = await run("start", identifier, "--request", str(request), "--confirm", "--wait")
    assert started.returncode == 0, started.stdout + started.stderr
    saved = json.loads(started.stdout)
    command_id = saved["command_id"]
    assert saved["outcome"] == "completed"
    for action, args in [
        ("get", [command_id]),
        ("inspect", [command_id]),
        ("by-key", ["cli"]),
        ("history", []),
        ("read", [command_id]),
    ]:
        result = await run(action, identifier, *args)
        assert result.returncode == 0, result.stdout + result.stderr
    failure = tmp_path / "failure.json"
    failure.write_text(
        json.dumps(
            {
                "request_key": "failed",
                "command": {"argv": ["/usr/bin/python3", "-c", "raise SystemExit(7)"]},
            }
        )
    )
    failed = await run("start", identifier, "--request", str(failure), "--confirm", "--wait")
    assert failed.returncode == 1 and json.loads(failed.stdout)["exit_code"] == 7
    missing = await run("by-key", identifier, "not-created")
    assert missing.returncode == 2 and json.loads(missing.stdout) is None
    invalid = await run("history", identifier, "--limit", "0")
    assert invalid.returncode != 0


def test_client_rejects_path_injection_and_nonfinite_wait_before_network():
    with KrimClient("http://127.0.0.1:1") as client:
        for call in [
            lambda: client.agent_command("a/b", "id"),
            lambda: client.agent_command("agent", "../escape"),
            lambda: client.agent_command_wait("agent", "id", timeout_seconds=float("nan")),
            lambda: client.agent_command_by_key("agent", "bad\nkey"),
        ]:
            with pytest.raises(ClientError) as error:
                call()
            assert error.value.status == 422


async def test_tcp_disconnection_before_acceptance_keeps_one_original_request(command_server):
    url, _root, app = command_server

    def create():
        with KrimClient(url, "test-key") as client:
            return configure(client)

    identifier = await asyncio.to_thread(create)
    entered, release = asyncio.Event(), asyncio.Event()

    async def hold(definition, _context):
        if definition.name == "command_execute":
            entered.set()
            await release.wait()

    app.state.agents._handle(identifier).runtime.tools.add_global_pre(hold)
    path = f"/v1/agents/{identifier}/commands"
    payload = {
        "request_key": "disconnected",
        "command": {"argv": ["/usr/bin/python3", "-c", "print('once')"]},
    }
    async with httpx.AsyncClient(
        base_url=url, headers={"Authorization": "Bearer test-key"}, trust_env=False
    ) as client:
        first = asyncio.create_task(client.post(path, params={"confirmed": True}, json=payload))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
            second = asyncio.create_task(
                client.post(path, params={"confirmed": True}, json=payload)
            )
            release.set()
            accepted = await asyncio.wait_for(second, 10)
            assert accepted.status_code == 202
            identifier_command = accepted.json()["command_id"]
            result = await client.post(path + "/" + identifier_command + "/wait")
            assert (
                result.json()["outcome"] == "completed" and result.json()["stdout_text"] == "once\n"
            )
            history = (await client.get(path)).json()["entries"]
            assert len(history) == 1 and history[0]["command_id"] == identifier_command
        finally:
            release.set()
