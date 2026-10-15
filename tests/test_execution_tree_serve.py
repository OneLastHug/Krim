"""真实serve显式配置树根与限额，公共命令及同原统计查询。"""

import asyncio
import json
import os
import socket
import sys

import httpx
from test_execution_tree_quotas import tree_root as tree_root


async def test_actual_serve_uses_explicit_cgroup_root_and_resource_view(tmp_path, tree_root):
    limits = tmp_path / "limits.json"
    limits.write_text(
        json.dumps({"tree": {"cpu_seconds": 1.0, "memory_bytes": 134217728, "process_count": 8}})
    )
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        "-m",
        "krim_agent",
        "serve",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--execution",
        "--execution-limits",
        str(limits),
        "--execution-cgroup-root",
        str(tree_root.path),
        env={
            **os.environ,
            "KRIM_DATABASE_URL": f"sqlite+aiosqlite:///{tmp_path / 'serve.sqlite'}",
            "KRIM_API_KEY": "tree-test-key",
            "KRIM_AGENT_INTERACTIVE_TOOLS": "true",
        },
        cwd=str(tmp_path),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}", headers={"Authorization": "Bearer tree-test-key"}
        ) as api:
            for _ in range(150):
                if child.returncode is not None:
                    _, error = await child.communicate()
                    raise AssertionError(error.decode())
                try:
                    if (await api.get("/healthz")).is_success:
                        break
                except httpx.TransportError:
                    pass
                await asyncio.sleep(0.05)
            response = await api.put(
                "/v1/agents/profiles/tree-serve",
                json={
                    "expected_revision": 0,
                    "config": {
                        "name": "tree-serve",
                        "description": "实际树限额启动",
                        "execution_key": "local",
                        "tool_names": ["command_execute", "command_get", "command_resources"],
                    },
                },
            )
            assert response.status_code == 200, response.text
            response = await api.post("/v1/agents", json={"name": "tree-serve"})
            assert response.status_code == 201, response.text
            agent_id = response.json()["agent_id"]
            base = f"/v1/agents/{agent_id}/commands"
            request = {
                "request_key": "serve-tree",
                "command": {"argv": ["/usr/bin/python3", "-c", "print('tree')"]},
            }
            accepted = await api.post(base, params={"confirmed": "true"}, json=request)
            assert accepted.status_code == 202, accepted.text
            command_id = accepted.json()["command_id"]
            waited = await api.post(base + f"/{command_id}/wait", params={"timeout_seconds": 30})
            assert waited.status_code == 200, waited.text
            assert waited.json()["outcome"] == "completed" and waited.json()["processes_reaped"], (
                waited.text
            )
            resource = await api.get(base + f"/{command_id}/resources")
            assert resource.status_code == 200, resource.text
            value = resource.json()
            assert value["command_id"] == command_id and value["usage"]["membership_verified"]
            assert value["usage"]["populated"] is False and value["usage"]["leader_reaped"] is True
            assert value["limits"]["tree"]["process_count"] == 8
    finally:
        if child.returncode is None:
            child.terminate()
            _, error = await asyncio.wait_for(child.communicate(), 15)
            (tmp_path / "serve-stderr.log").write_bytes(error)
