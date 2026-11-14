from __future__ import annotations

import asyncio
import json
import subprocess
import sys

import pytest
from test_dev_terminal_client import command, execution, handle
from test_dev_terminal_worker import wait_server

from krim_agent.dev_terminal.client import WorkerClient
from krim_agent.dev_terminal.host import HostWorkerConnection


def start_host(state, project):
    server = subprocess.Popen([
        sys.executable, "-m", "krim_agent.dev_terminal.worker", "serve",
        "--state", str(state), "--project", str(project), "--trusted-attachments",
    ], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    wait_server(state, server)
    return server


@pytest.mark.asyncio
async def test_private_host_worker_survives_client_process_exit(tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    state = tmp_path / "state"
    server = start_host(state, project)
    try:
        client = WorkerClient(HostWorkerConnection(state))
        request = command(command="printf host-ready; cat; printf host-done", timeout_seconds=None,
                          lifecycle="project_service", wait_seconds=0)
        process = handle(request)
        await client.attach(request, process, execution(request, process))
        # A separate Python client connects after the first attach call ended.
        read = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "krim_agent.dev_terminal.worker", "rpc", "--state", str(state),
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await read.communicate(
            json.dumps(client.control(request, process, "inspect")).encode(),
        )
        assert read.returncode == 0, stderr
        assert json.loads(stdout)["result"]["state"] == "running"
        await client.invoke(client.control(request, process, "stdin", data="input\n", close=True))
        for _ in range(100):
            if (await client.inspect(request, process))["state"] == "exited":
                break
            await asyncio.sleep(0.02)
        page = await client.invoke(client.control(request, process, "output", cursor=0))
        assert page["text"] == "host-readyinput\nhost-done" and page["eof"]
    finally:
        server.terminate()
        server.wait(timeout=5)
        if server.stderr:
            server.stderr.close()
