"""真实CLI服务/TCP客户端/CLI命令及服务重启的命令管理演练。"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.client import KrimClient
from krim_agent.execution import CommandSpec
from krim_agent.sdk import AgentCommandRequest, CommandProposalRequest, WorkspaceChangeAction


def main() -> None:
    with TemporaryDirectory(prefix="krim-command-management-") as directory:
        root = Path(directory)
        files = root / "files"
        files.mkdir()
        (files / "input.txt").write_text("original input\n", encoding="utf-8")
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        url = f"http://127.0.0.1:{port}"
        environment = {
            key: value for key, value in os.environ.items() if not key.startswith("KRIM_AGENT_")
        }
        environment.pop("PYTHONPATH", None)
        environment.update(
            {
                "KRIM_AGENT_PROVIDER": "fake",
                "KRIM_AGENT_API_KEY": "command-trial-key",
                "KRIM_AGENT_DATABASE_URL": f"sqlite+aiosqlite:///{root / 'agent.sqlite'}",
                "KRIM_AGENT_INTERACTIVE_TOOLS": "true",
            }
        )
        process: subprocess.Popen[bytes] | None = None

        def start() -> None:
            nonlocal process
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-I",
                    "-m",
                    "krim_agent",
                    "serve",
                    "--port",
                    str(port),
                    "--workspace",
                    str(files),
                    "--workspace-write",
                    "--execution",
                ],
                env=environment,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            deadline = time.monotonic() + 15
            with httpx.Client(trust_env=False, timeout=1) as http:
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        raise RuntimeError("命令演练服务启动失败")
                    try:
                        if http.get(url + "/healthz").is_success:
                            return
                    except httpx.HTTPError:
                        pass
                    time.sleep(0.05)
            raise RuntimeError("命令演练服务未及时就绪")

        def stop() -> None:
            nonlocal process
            if process is None:
                return
            process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
                raise RuntimeError("命令演练服务未收束") from None
            process = None

        def cli(*arguments: str) -> dict[str, object]:
            result = subprocess.run(
                [
                    sys.executable,
                    "-I",
                    "-m",
                    "krim_agent",
                    "commands",
                    *arguments,
                    "--url",
                    url,
                ],
                env=environment,
                capture_output=True,
                text=True,
                timeout=40,
            )
            if result.returncode != 0:
                raise RuntimeError(result.stderr or result.stdout)
            return json.loads(result.stdout)

        try:
            start()
            with KrimClient(url, "command-trial-key") as client:
                client.agent_profile_put(
                    AgentProfileUpdate(
                        expected_revision=0,
                        config=AgentProfile(
                            name="command-worker",
                            description="命令管理演练",
                            persistent=True,
                            workspace_key="local",
                            execution_key="local",
                            tool_names=[
                                "workspace_read",
                                "workspace_change",
                                "workspace_reconcile",
                                "workspace_finalize",
                                "command_execute",
                                "command_get",
                                "command_read",
                                "command_cancel",
                                "command_history",
                                "command_inspect",
                                "command_changes",
                                "command_propose",
                            ],
                        ),
                    )
                )
                agent_id = client.agent_spawn("command-worker", persistent=True).agent_id
                request = AgentCommandRequest(
                    request_key="trial",
                    input_paths=["input.txt"],
                    command=CommandSpec(
                        argv=[
                            "/usr/bin/python3",
                            "-I",
                            "-c",
                            "from pathlib import Path;print(Path('input.txt').read_text(),end='');"
                            "Path('output.txt').write_text('artifact\\n')",
                        ]
                    ),
                )
                request_path = root / "command.json"
                request_path.write_text(request.model_dump_json(), encoding="utf-8")
                accepted = cli("start", agent_id, "--request", str(request_path), "--confirm")
                command_id = str(accepted["command_id"])
                result = cli("wait", agent_id, command_id)
                assert (
                    result["outcome"] == "completed" and result["stdout_text"] == "original input\n"
                )
                assert client.agent_command_history(agent_id).entries[0].command_id == command_id
                assert (
                    client.agent_command_inspect(agent_id, command_id).request.command
                    == request.command
                )
                proposal = client.agent_command_propose(
                    agent_id,
                    CommandProposalRequest(
                        command_id=command_id, path="output.txt", request_key="proposal"
                    ),
                )
                applied = client.agent_workspace_change(
                    agent_id,
                    WorkspaceChangeAction(
                        proposal_id=proposal.proposal.proposal_id,
                        action="apply",
                        expected_revision=0,
                        request_key="apply",
                    ),
                    confirmed=True,
                )
                assert (
                    applied.status == "done" and (files / "output.txt").read_text() == "artifact\n"
                )
                operation_id = applied.operation_id
            stop()
            (files / "input.txt").write_text("external input\n")
            (files / "output.txt").write_text("external output\n")
            start()
            with KrimClient(url, "command-trial-key") as client:
                assert client.agent(agent_id).agent_id == agent_id
                original = client.agent_command_by_key(agent_id, "trial")
                assert (
                    original is not None
                    and original.command_id == command_id
                    and original.stdout_text == "original input\n"
                )
                assert cli("get", agent_id, command_id)["command_id"] == command_id
                replay = client.agent_command_start(agent_id, request, confirmed=True)
                assert replay.command_id == command_id
                assert (
                    client.agent_command_read(
                        agent_id, command_id, source="artifact", path="output.txt"
                    ).text
                    == "artifact\n"
                )
                old_operation = client.agent_workspace_change(
                    agent_id,
                    WorkspaceChangeAction(
                        proposal_id=proposal.proposal.proposal_id,
                        action="apply",
                        expected_revision=0,
                        request_key="apply",
                    ),
                    confirmed=True,
                )
                assert (
                    old_operation.operation_id == operation_id
                    and (files / "output.txt").read_text() == "external output\n"
                )
            stop()
            print(
                json.dumps(
                    {
                        "agent_id": agent_id,
                        "command_id": command_id,
                        "operation_id": operation_id,
                        "real_cli_service": True,
                        "cli_and_client": True,
                        "restored_original": True,
                        "external_files_preserved": True,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
        finally:
            stop()


if __name__ == "__main__":
    main()
