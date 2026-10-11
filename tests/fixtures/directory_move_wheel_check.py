"""项目外安装包目录移动：TCP/Client/CLI、强退冷读取与原inode撤销。"""

import json
import os
import runpy
import sqlite3
import subprocess
import sys
from pathlib import Path

import httpx

import krim_agent
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.client import KrimClient
from krim_agent.sdk import DirectoryChangeAction, DirectoryChangeSpec, DirectoryPreviewRequest


def main():
    assert "/site-packages/" in str(krim_agent.__file__)
    support = runpy.run_path(str(Path(__file__).with_name("directory_public_wheel_check.py")))
    server, cli, key = support["server"], support["cli"], support["KEY"]
    folder = Path(sys.argv[1]).resolve()
    legacy_python = sys.argv[2] if len(sys.argv) > 2 else None
    folder.mkdir()
    root = folder / "workspace"
    root.mkdir()
    (root / "source").mkdir()
    (root / "target").mkdir()
    (root / "source" / "empty").mkdir(mode=0o750)
    inode = (root / "source" / "empty").stat().st_ino
    database = folder / "state.sqlite"
    env = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("KRIM_AGENT_") and name != "PYTHONPATH"
    }
    env.update(
        KRIM_AGENT_DATABASE_URL=f"sqlite+aiosqlite:///{database}",
        KRIM_AGENT_API_KEY=key,
        KRIM_AGENT_INTERACTIVE_TOOLS="true",
    )
    with server(folder, env, 1) as (url, process), KrimClient(url, key) as client:
        client.agent_profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name="directory-move-wheel",
                    description="目录移动安装包演练",
                    workspace_key="local",
                    tool_names=support["NAMES"],
                    persistent=True,
                ),
            )
        )
        agent = client.agent_spawn("directory-move-wheel", persistent=True).agent_id
        if legacy_python:
            legacy_agent = client.agent_spawn("directory-move-wheel", persistent=True).agent_id
            legacy = subprocess.run(
                [
                    legacy_python,
                    "-I",
                    str(Path(__file__).with_name("directory_legacy_client_check.py")),
                    url,
                    legacy_agent,
                    key,
                ],
                capture_output=True,
                text=True,
                timeout=20,
            )
            assert legacy.returncode == 0, legacy.stderr
            assert json.loads(legacy.stdout)["ordinary_response_compatible"]
        before = cli(url, env, "inspect", agent, "source/empty")
        request = DirectoryPreviewRequest(
            spec=DirectoryChangeSpec(
                path="source/empty",
                operation="move",
                target_path="target/moved",
                expected_fingerprint=before["fingerprint"],
            ),
            request_key="move-preview",
        )
        intent = folder / "intent.json"
        intent.write_text(request.model_dump_json(), encoding="utf-8")
        proposal = cli(url, env, "preview", agent, "--request", intent)
        assert proposal["target_path"] == "target/moved"
        assert client.agent_directory_preview(agent, request).proposal_id == proposal["proposal_id"]
        action = DirectoryChangeAction(
            proposal_id=proposal["proposal_id"],
            action="apply",
            expected_revision=0,
            request_key="move-apply",
        )
        applied = client.agent_directory_change(agent, action, confirmed=True)
        assert applied.status == "done"
        assert client.agent_directory_change(agent, action, confirmed=True) == applied
        assert (root / "target" / "moved").stat().st_ino == inode
        assert not (root / "source" / "empty").exists()
        with httpx.Client(base_url=url, headers={"Authorization": f"Bearer {key}"}) as api:
            script = api.get("/ui/directory-changes-ui.js")
            assert script.status_code == 200 and "targetPath" in script.text
            assert "移动空目录" in script.text and "proposalPath" in script.text
        process.kill()
        process.wait(5)
    # 仅对已强退合成库加速原handle租约过期；不接触默认库。
    with sqlite3.connect(database) as connection:
        connection.execute("UPDATE agent_handles SET lease_expires_at=0")
    with server(folder, env, 2) as (url, _process), KrimClient(url, key) as client:
        current = client.agent_directory_query(
            agent, kind="proposal_by_key", request_key="move-preview"
        ).proposal
        assert current.target_path == "target/moved" and current.status == "applied"
        saved = cli(url, env, "operation-by-key", agent, "move-apply")["operation"]
        assert saved["operation_id"] == applied.operation_id and saved["status"] == "done"
        assert (root / "target" / "moved").stat().st_ino == inode
        kept = cli(
            url,
            env,
            "keep",
            agent,
            current.proposal_id,
            "--revision",
            current.revision,
            "--request-key",
            "move-keep",
            "--confirm",
        )
        assert kept["status"] == "done" and not kept["receipt"]["changed"]
        current = client.agent_directory_query(
            agent, kind="proposal", identifier=current.proposal_id
        ).proposal
        assert current.status == "kept"
        undone = cli(
            url,
            env,
            "undo",
            agent,
            current.proposal_id,
            "--revision",
            current.revision,
            "--request-key",
            "move-undo",
            "--confirm",
        )
        assert undone["status"] == "done"
        assert (root / "source" / "empty").stat().st_ino == inode
        assert not (root / "target" / "moved").exists()
        history = cli(url, env, "history", agent, current.proposal_id)["history"]
        assert [item["action"] for item in history] == ["apply", "keep", "undo"]
        assert (
            cli(url, env, "reconcile", agent, undone["operation_id"], "--confirm")["status"]
            == "done"
        )
        assert not cli(url, env, "finalize", agent, undone["operation_id"], "--confirm")[
            "cleanup_pending"
        ]
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT count(*) FROM workspace_directory_proposals").fetchone()[
            0
        ] == (2 if legacy_python else 1)
        assert (
            connection.execute("SELECT count(*) FROM workspace_directory_attempts").fetchone()[0]
            == 3
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
                "move_rest_client_cli": True,
                "static_move_ui": True,
                "service_killed": True,
                "cold_read_without_replay": True,
                "keep_without_write": True,
                "original_inode_undo": True,
                "legacy_client_ordinary_compatible": legacy_python is not None,
                "proposals": 2 if legacy_python else 1,
                "attempts": 3,
            }
        )
    )


if __name__ == "__main__":
    main()
