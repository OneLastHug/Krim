"""项目外安装包的真实REST/Client/CLI与强退后原事实冷读取。"""

import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import httpx

import krim_agent
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.client import KrimClient
from krim_agent.sdk import (
    DirectoryChangeAction,
    DirectoryChangeSpec,
    DirectoryPreviewRequest,
)

KEY = "wheel-directory-synthetic-key"
NAMES = [
    "directory_inspect",
    "directory_preview",
    "directory_change",
    "directory_query",
    "directory_reconcile",
    "directory_finalize",
]


def cli(url, env, *args):
    value = subprocess.run(
        [sys.executable, "-I", "-m", "krim_agent", "directories", *map(str, args), "--url", url],
        capture_output=True,
        text=True,
        env=env,
        timeout=25,
    )
    assert value.returncode == 0, value.stderr
    return json.loads(value.stdout)


@contextmanager
def server(folder, env, index):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    with (folder / f"server-{index}.log").open("w") as log:
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
                str(folder / "workspace"),
                "--workspace-write",
            ],
            cwd=folder,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 20
            with httpx.Client(trust_env=False, timeout=0.5) as probe:
                while True:
                    assert process.poll() is None, "隔离测试服务启动失败，查看server日志"
                    try:
                        if probe.get(url + "/healthz").status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    assert time.monotonic() < deadline, "隔离服务启动超时"
                    time.sleep(0.05)
            yield url, process
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(5)


def main():
    assert "/site-packages/" in str(krim_agent.__file__)
    folder = Path(sys.argv[1]).resolve()
    folder.mkdir()
    root = folder / "workspace"
    root.mkdir()
    database = folder / "state.sqlite"
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("KRIM_AGENT_") and key != "PYTHONPATH"
    }
    env.update(
        KRIM_AGENT_DATABASE_URL=f"sqlite+aiosqlite:///{database}",
        KRIM_AGENT_API_KEY=KEY,
        KRIM_AGENT_INTERACTIVE_TOOLS="true",
    )
    with server(folder, env, 1) as (url, process), KrimClient(url, KEY) as client:
        client.agent_profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name="directory-wheel",
                    description="目录安装包演练",
                    workspace_key="local",
                    tool_names=NAMES,
                    persistent=True,
                ),
            )
        )
        agent = client.agent_spawn("directory-wheel", persistent=True).agent_id
        missing = cli(url, env, "inspect", agent, "empty")
        assert not missing["found"] and missing["fingerprint"] is None
        intent = folder / "intent.json"
        intent.write_text(
            DirectoryPreviewRequest(
                spec=DirectoryChangeSpec(
                    path="empty",
                    operation="create",
                    mode=0o750,
                ),
                request_key="preview",
            ).model_dump_json()
        )
        proposed = cli(url, env, "preview", agent, "--request", intent)
        action = DirectoryChangeAction(
            proposal_id=proposed["proposal_id"],
            action="apply",
            expected_revision=proposed["revision"],
            request_key="apply",
        )
        result = client.agent_directory_change(agent, action, confirmed=True)
        assert result.status == "done" and root.joinpath("empty").is_dir()
        inspected = client.agent_directory_inspect(agent, "empty")
        assert inspected.found and inspected.empty and result.receipt is not None
        assert inspected.fingerprint == result.receipt.after_fingerprint
        identity = root.joinpath("empty").stat().st_ino
        assert (
            cli(url, env, "operation-by-key", agent, "apply")["operation"]["operation_id"]
            == result.operation_id
        )
        process.kill()
        process.wait(5)
    # 只在已强退的合成测试库加速租约过期；不修改业务数据库。
    with sqlite3.connect(database) as connection:
        connection.execute("UPDATE agent_handles SET lease_expires_at=0")
    with server(folder, env, 2) as (url, _process), KrimClient(url, KEY) as client:
        original = client.agent_directory_query(
            agent, kind="operation_by_key", request_key="apply"
        ).operation
        assert original.operation_id == result.operation_id and original.status == "done"
        assert root.joinpath("empty").stat().st_ino == identity
        page = cli(url, env, "list", agent)
        assert page["proposals"][0]["proposal_id"] == proposed["proposal_id"]
        assert (
            cli(url, env, "history", agent, proposed["proposal_id"])["history"][0]["operation_id"]
            == result.operation_id
        )
        current = client.agent_directory_query(
            agent, kind="proposal", identifier=proposed["proposal_id"]
        ).proposal
        undone = cli(
            url,
            env,
            "undo",
            agent,
            proposed["proposal_id"],
            "--revision",
            current.revision,
            "--request-key",
            "undo",
            "--confirm",
        )
        assert undone["status"] == "done" and not root.joinpath("empty").exists()
        assert (
            cli(url, env, "reconcile", agent, undone["operation_id"], "--confirm")["status"]
            == "done"
        )
        assert not cli(url, env, "finalize", agent, undone["operation_id"], "--confirm")[
            "cleanup_pending"
        ]
    with sqlite3.connect(database) as connection:
        assert (
            connection.execute("SELECT count(*) FROM workspace_directory_proposals").fetchone()[0]
            == 1
        )
        assert (
            connection.execute("SELECT count(*) FROM workspace_directory_attempts").fetchone()[0]
            == 2
        )
        assert (
            connection.execute("SELECT version_num FROM alembic_version").fetchone()[0]
            == "0040_mixed_batch_model_sources"
        )
        assert not connection.execute("PRAGMA foreign_key_check").fetchall()
    print(
        json.dumps(
            {
                "package": krim_agent.__file__,
                "rest_client_cli": True,
                "directory_inspection": True,
                "service_killed": True,
                "cold_read_without_replay": True,
                "undo": True,
                "proposals": 1,
                "attempts": 2,
            }
        )
    )


if __name__ == "__main__":
    main()
