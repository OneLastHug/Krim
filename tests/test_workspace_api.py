"""工作区公开HTTP/client/CLI、真实TCP和跨进程恢复；全部使用临时文件。"""

import asyncio
import json
import os
import socket
import sys
from pathlib import Path

import httpx
import pytest
import uvicorn
from pydantic import SecretStr

from krim_agent.api import create_app
from krim_agent.client import ClientError, KrimClient

pytestmark = pytest.mark.skipif(os.name != "posix", reason="需要POSIX工作区文件边界")


async def test_http_root_cannot_be_selected_and_auth_and_subset_are_enforced(runtime, tmp_path):
    (tmp_path / "file.txt").write_text("needle\n", encoding="utf-8")
    (tmp_path / ".env").write_text("hidden", encoding="utf-8")
    runtime.settings = runtime.settings.model_copy(update={"api_key": SecretStr("workspace-key")})
    app = create_app(runtime=runtime, workspace_root=tmp_path)
    headers = {"Authorization": "Bearer workspace-key"}
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api,
    ):
        view = (await api.post("/v1/agents", headers=headers, json={"name": "default"})).json()
        path = f"/v1/agents/{view['agent_id']}/workspace"
        assert (await api.get(path)).status_code == 401
        info = (await api.get(path, headers=headers)).json()
        assert info["key"] == "local" and str(tmp_path) not in json.dumps(info)
        listing = (await api.get(f"{path}/list", headers=headers)).json()
        assert [entry["path"] for entry in listing["entries"]] == ["file.txt"]
        file = (await api.get(f"{path}/read", params={"path": "file.txt"}, headers=headers)).json()
        assert file["content"] == "needle\n" and len(file["sha256"]) == 64
        search = await api.post(f"{path}/search", json={"query": "needle"}, headers=headers)
        assert search.status_code == 200 and search.json()["matches"][0]["line"] == 1
        assert (
            await api.post(f"{path}/search", json={"query": "x", "root": "/tmp"}, headers=headers)
        ).status_code == 422
        assert (
            await api.get(f"{path}/read", params={"path": "../file.txt"}, headers=headers)
        ).status_code == 422
        assert (
            await api.get(f"{path}/read", params={"path": ".env"}, headers=headers)
        ).status_code == 403
        config = {
            "name": "readonly",
            "description": "只读子集",
            "workspace_key": "local",
            "tool_names": ["workspace_read"],
        }
        assert (
            await api.put(
                "/v1/agents/profiles/readonly",
                json={"expected_revision": 0, "config": config},
                headers=headers,
            )
        ).status_code == 200
        reader = (await api.post("/v1/agents", json={"name": "readonly"}, headers=headers)).json()
        assert (
            await api.post(
                f"/v1/agents/{reader['agent_id']}/workspace/search",
                json={"query": "needle"},
                headers=headers,
            )
        ).status_code == 403
        config.update(
            {"name": "bad", "workspace_key": "unregistered", "workspace_root": str(tmp_path)}
        )
        assert (
            await api.put(
                "/v1/agents/profiles/bad",
                json={"expected_revision": 0, "config": config},
                headers=headers,
            )
        ).status_code == 422


async def test_real_tcp_client_and_cli_read_the_same_workspace(runtime, tmp_path):
    (tmp_path / "file.txt").write_text("needle\nsecond\n", encoding="utf-8")
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    port = listener.getsockname()[1]
    app = create_app(runtime=runtime, workspace_root=tmp_path)
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    task = asyncio.create_task(server.serve(sockets=[listener]))
    url = f"http://127.0.0.1:{port}"
    try:
        async with asyncio.timeout(10):
            while not server.started:
                if task.done():
                    await task
                await asyncio.sleep(0.01)

        def client_check():
            with KrimClient(url) as client:
                agent = client.agent_spawn("default", persistent=True)
                assert client.agent_workspace(agent.agent_id).key == "local"
                file = client.agent_read_file(agent.agent_id, "file.txt", start_line=2, max_lines=1)
                assert file.content == "second\n"
                assert client.agent_list_files(agent.agent_id).entries[0].path == "file.txt"
                assert client.agent_search_files(agent.agent_id, "needle").matches[0].line == 1
                with pytest.raises(ClientError) as error:
                    client.agent_read_file(agent.agent_id, "../file.txt")
                assert error.value.code == "workspace_path"
                return agent.agent_id

        identifier = await asyncio.to_thread(client_check)
        for action, extra in [
            ("info", []),
            ("list", []),
            ("read", ["file.txt"]),
            ("search", ["needle"]),
        ]:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "krim_agent",
                "workspace",
                action,
                identifier,
                *extra,
                "--url",
                url,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), 10)
            assert process.returncode == 0, stderr.decode()
            value = json.loads(stdout)
            if action == "read":
                assert value["content"] == "needle\nsecond\n"
            elif action == "search":
                assert value["matches"][0]["line"] == 1
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 10)
        listener.close()


async def test_workspace_restores_in_real_new_python_process_without_replaying_model(tmp_path):
    root = tmp_path / "files"
    root.mkdir()
    (root / "file.txt").write_text("persistent contents", encoding="utf-8")
    database = tmp_path / "agents.sqlite"

    async def worker(mode, identifier=""):
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(Path(__file__).parent / "fixtures/workspace_agent_worker.py"),
            str(database),
            str(root),
            mode,
            identifier,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), 30)
        assert process.returncode == 0, stderr.decode()
        return json.loads(stdout)

    first = await worker("create")
    second = await worker("restore", first["agent_id"])
    assert second["agent_id"] == first["agent_id"]
    assert second["run_id"] == first["run_id"]
    assert second["sha256"] == first["sha256"]
    assert first["calls"] == 2 and second["calls"] == 0
