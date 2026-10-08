"""本地可重复试运行：真实CLI服务/TCP、Agent恢复、任务状态和eval。"""

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
from krim_agent.task_state import TaskWriteRequest


def main() -> None:
    with TemporaryDirectory(prefix="krim-local-trial-") as directory:
        root = Path(directory)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        url = f"http://127.0.0.1:{port}"
        environment = {
            key: value for key, value in os.environ.items() if not key.startswith("KRIM_AGENT_")
        }
        environment.update(
            {
                "KRIM_AGENT_PROVIDER": "fake",
                "KRIM_AGENT_DATABASE_URL": f"sqlite+aiosqlite:///{root / 'agent.sqlite'}",
                "KRIM_AGENT_API_KEY": "trial-local-key",
            }
        )
        process: subprocess.Popen[bytes] | None = None

        def start() -> None:
            nonlocal process
            process = subprocess.Popen(
                [sys.executable, "-m", "krim_agent", "serve", "--port", str(port)],
                env=environment,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            deadline = time.monotonic() + 15
            with httpx.Client(trust_env=False, timeout=1) as http:
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        raise RuntimeError("本地试运行服务启动失败")
                    try:
                        if http.get(url + "/healthz").is_success:
                            return
                    except httpx.HTTPError:
                        pass
                    time.sleep(0.05)
            raise RuntimeError("本地试运行服务未及时就绪")

        def stop() -> None:
            nonlocal process
            if process is None:
                return
            process.send_signal(signal.SIGINT if os.name != "nt" else signal.SIGTERM)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
                raise RuntimeError("试运行服务未正常收束") from None
            process = None

        try:
            start()
            with KrimClient(url, "trial-local-key") as client:
                client.agent_profile_put(
                    AgentProfileUpdate(
                        expected_revision=0,
                        config=AgentProfile(
                            name="trial_worker",
                            description="本地演练",
                            tool_names=["add", "echo"],
                            persistent=True,
                        ),
                    )
                )
                agent = client.agent_spawn("trial_worker")
                tasks = client.agent_tasks_write(
                    agent.agent_id,
                    "plan",
                    TaskWriteRequest(
                        operation="replace",
                        items=["计算", "回声"],
                        expected_revision=0,
                        request_key="trial-plan",
                    ),
                )
                client.agent_start(agent.agent_id, "/add 2 3", "trial-first")
                first = client.agent_wait(agent.agent_id)
                assert first.status == "completed" and first.result is not None
                assert '"sum": 5.0' in first.result.text
                client.agent_tasks_write(
                    agent.agent_id,
                    "plan",
                    TaskWriteRequest(
                        operation="complete",
                        item_id=tasks.items[0].id,
                        expected_revision=1,
                        request_key="trial-done",
                    ),
                )
                second = client.agent_resume(agent.agent_id, "/echo ready", "trial-second")
                assert second.turn == 2 and second.status == "completed"
                identifier, original_run = agent.agent_id, first.current_run_id
                assert original_run is not None
            # 正常关闭释放owner，再用相同SQLite文件恢复原实例。
            stop()
            start()
            with KrimClient(url, "trial-local-key") as client:
                restored = client.agent(identifier)
                assert restored.turn == 2 and restored.profile_revision == 1
                plan = client.agent_tasks(identifier, "plan")
                assert plan.revision == 2 and plan.items[0].completed
                frames = list(client.agent_events(identifier, run_id=original_run))
                assert frames[-1].kind == "end" and {frame.stream_id for frame in frames} == {
                    original_run
                }
                closed = client.agent_session_close(
                    identifier, client.agent_session(identifier).revision
                )
                client.agent_session_reopen(identifier, closed.revision)
                assert client.agent_destroy(identifier).status == "destroyed"
            stop()
            dataset = root / "eval.json"
            dataset.write_text(
                json.dumps(
                    {
                        "name": "local-trial",
                        "cases": [{"id": "sum", "input": "/add 2 3", "expected": "5.0"}],
                    }
                ),
                encoding="utf-8",
            )
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "krim_agent",
                    "eval",
                    "run",
                    str(dataset),
                    "--scorer",
                    "contains",
                    "--database",
                    str(root / "eval.sqlite"),
                    "--output",
                    str(root / "report.json"),
                ],
                env=environment,
                capture_output=True,
                check=False,
                timeout=30,
            )
            assert result.returncode == 0, result.stderr.decode()
            print(
                json.dumps(
                    {
                        "service": "real CLI/TCP",
                        "agent_turns": 2,
                        "task_revision": 2,
                        "restored": True,
                        "history_replayed": True,
                        "eval_passed": True,
                    },
                    ensure_ascii=False,
                )
            )
        finally:
            if process is not None:
                stop()


if __name__ == "__main__":
    main()
