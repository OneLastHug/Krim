"""真实CLI服务骤停后，公开接口与CLI保留原命令的未知事实。"""

import json
import os
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import time

import httpx
import pytest

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.client import KrimClient
from krim_agent.execution import CommandSpec
from krim_agent.sdk import AgentCommandRequest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实Linux服务进程",
)


def test_public_cli_reports_unknown_after_server_kill_and_never_replays(tmp_path):
    database = tmp_path / "commands.sqlite"
    with socket.socket() as socket_port:
        socket_port.bind(("127.0.0.1", 0))
        port = socket_port.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    environment = {
        key: value for key, value in os.environ.items() if not key.startswith("KRIM_AGENT_")
    }
    environment.pop("PYTHONPATH", None)
    environment.update(
        {
            "KRIM_AGENT_DATABASE_URL": f"sqlite+aiosqlite:///{database}",
            "KRIM_AGENT_API_KEY": "crash-test",
            "KRIM_AGENT_INTERACTIVE_TOOLS": "true",
            "KRIM_AGENT_PROVIDER": "fake",
        }
    )

    def start():
        process = subprocess.Popen(
            [sys.executable, "-I", "-m", "krim_agent", "serve", "--port", str(port), "--execution"],
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        deadline = time.monotonic() + 15
        with httpx.Client(trust_env=False, timeout=0.5) as http:
            while time.monotonic() < deadline:
                assert process.poll() is None
                try:
                    if http.get(url + "/healthz").is_success:
                        return process
                except httpx.HTTPError:
                    pass
                time.sleep(0.05)
        process.kill()
        process.wait()
        raise AssertionError("server not ready")

    process = start()
    try:
        with KrimClient(url, "crash-test") as client:
            client.agent_profile_put(
                AgentProfileUpdate(
                    expected_revision=0,
                    config=AgentProfile(
                        name="worker",
                        description="crash recovery",
                        execution_key="local",
                        persistent=True,
                        tool_names=[
                            "command_execute",
                            "command_get",
                            "command_history",
                            "command_inspect",
                            "command_read",
                            "command_cancel",
                        ],
                        tool_timeout_seconds=60,
                    ),
                )
            )
            identifier = client.agent_spawn("worker", persistent=True).agent_id
            request = AgentCommandRequest(
                request_key="original",
                command=CommandSpec(
                    argv=[
                        "/usr/bin/python3",
                        "-c",
                        "import time;print('ready',flush=True);time.sleep(30)",
                    ]
                ),
            )
            accepted = client.agent_command_start(identifier, request, confirmed=True)
            deadline = time.monotonic() + 10
            while True:
                original = client.agent_command(identifier, accepted.command_id)
                if original.started and original.stdout_text == "ready\n":
                    break
                assert time.monotonic() < deadline and original.phase not in {
                    "finished",
                    "interrupted",
                }
                time.sleep(0.05)
        process.kill()
        process.wait(timeout=5)
        process.stderr.close()
        # 只有确认原进程死亡后才加速测试租约；不据文件/超时猜测活跃owner已失效。
        with sqlite3.connect(database) as connection:
            connection.execute("UPDATE agent_handles SET lease_expires_at=0")
            connection.execute("UPDATE command_executions SET lease_expires_at=0")
        process = start()
        with KrimClient(url, "crash-test") as client:
            restored = client.agent_command_by_key(identifier, "original")
            assert restored.command_id == accepted.command_id
            assert (
                restored.phase == "interrupted"
                and restored.outcome == "uncertain"
                and not restored.processes_reaped
            )
            assert restored.stdout_text == "ready\n"
            assert (
                client.agent_command_start(identifier, request, confirmed=True).command_id
                == accepted.command_id
            )
            assert len(client.agent_command_history(identifier).entries) == 1
        result = subprocess.run(
            [
                sys.executable,
                "-I",
                "-m",
                "krim_agent",
                "commands",
                "get",
                identifier,
                accepted.command_id,
                "--url",
                url,
            ],
            env=environment,
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert result.returncode == 3, result.stdout + result.stderr
        assert json.loads(result.stdout)["outcome"] == "uncertain"
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if process.stderr is not None:
            process.stderr.close()
