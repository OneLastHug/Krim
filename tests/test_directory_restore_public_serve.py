"""实际serve显式开关、公开审批与持久原计划重开读回。"""

import asyncio
import os
import socket
import sys

import httpx

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.sdk import DirectoryRestoreExecute, DirectoryRestorePreview, DirectoryRestoreTarget
from krim_agent.workspace_directory_restore_control import DIRECTORY_RESTORE_STEP_TOOLS


async def test_actual_serve_explicit_flag_public_write_and_restart_same_plan(tmp_path):
    files = tmp_path / "files"
    (files / "tree/nested").mkdir(parents=True)
    (files / "tree/nested/nul").write_bytes(b"\0\xff")
    (files / "tree/nested/nul").chmod(0o640)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    command = [
        sys.executable,
        "-I",
        "-m",
        "krim_agent",
        "serve",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--workspace",
        str(files),
        "--workspace-write",
        "--directory-checkpoints",
        "--directory-restores",
    ]
    environment = {
        **os.environ,
        "KRIM_AGENT_DATABASE_URL": f"sqlite+aiosqlite:///{tmp_path / 'serve.sqlite'}",
        "KRIM_AGENT_PROVIDER": "fake",
        "KRIM_AGENT_API_KEY": "synthetic-directory-restore-key",
        "KRIM_AGENT_INTERACTIVE_TOOLS": "true",
    }

    async def launch():
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=str(tmp_path),
            env=environment,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        return process

    async def ready(api, process):
        async with asyncio.timeout(10):
            while True:
                if process.returncode is not None:
                    _, errors = await process.communicate()
                    raise AssertionError(errors.decode())
                try:
                    if (await api.get("/healthz")).is_success:
                        return
                except httpx.TransportError:
                    pass
                await asyncio.sleep(0.05)

    process = await launch()
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}",
            headers={"Authorization": "Bearer synthetic-directory-restore-key"},
        ) as api:
            await ready(api, process)
            profile = AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name="restore-serve",
                    description="本机完整目录恢复",
                    persistent=True,
                    workspace_key="local",
                    tool_names=[
                        "workspace_directory_checkpoint_capture",
                        *DIRECTORY_RESTORE_STEP_TOOLS.values(),
                    ],
                ),
            )
            response = await api.put(
                "/v1/agents/profiles/restore-serve", json=profile.model_dump(mode="json")
            )
            assert response.status_code == 200, response.text
            created = await api.post("/v1/agents", json={"name": "restore-serve"})
            assert created.status_code == 201, created.text
            agent_id = created.json()["agent_id"]
            checkpoints = f"/v1/agents/{agent_id}/workspace/directory-checkpoints"
            captured = await api.post(
                checkpoints, json={"root_path": "tree", "request_key": "target"}
            )
            assert captured.status_code == 202, captured.text
            captured = await api.post(
                checkpoints + f"/{captured.json()['checkpoint_id']}/wait",
                params={"timeout_seconds": 10},
            )
            assert captured.status_code == 200 and captured.json()["status"] == "available", (
                captured.text
            )
            (files / "tree/nested/nul").write_bytes(b"current\x80\0")
            base = f"/v1/agents/{agent_id}/workspace/directory-restores"
            wanted = DirectoryRestorePreview(
                target=DirectoryRestoreTarget(identifier=captured.json()["checkpoint_id"]),
                request_key="original-preview",
            )
            planned = await api.post(base + "/preview", json=wanted.model_dump(mode="json"))
            assert planned.status_code == 202, planned.text
            planned = await api.post(
                base + f"/{planned.json()['plan_id']}/wait", params={"timeout_seconds": 10}
            )
            assert planned.status_code == 200 and planned.json()["status"] == "ready", planned.text
            plan = planned.json()
            request = DirectoryRestoreExecute(
                plan_id=plan["plan_id"],
                expected_revision=plan["revision"],
                request_key="original-execute",
            )
            started = await api.post(
                base + "/start", params={"confirmed": "true"}, json=request.model_dump(mode="json")
            )
            assert started.status_code == 202, started.text
            finished = await api.post(
                base + f"/{plan['plan_id']}/wait", params={"timeout_seconds": 10}
            )
            assert finished.status_code == 200 and finished.json()["status"] == "done", (
                finished.text
            )
            value = finished.json()
            inode = (files / "tree/nested/nul").stat().st_ino
            assert (files / "tree/nested/nul").read_bytes() == b"\0\xff"
            process.terminate()
            await asyncio.wait_for(process.communicate(), 15)
            process = await launch()
            await ready(api, process)
            assert (await api.get(base + f"/{plan['plan_id']}")).json() == value
            assert (
                await api.get(base + "/by-key", params={"request_key": wanted.request_key})
            ).json() == value
            assert (
                await api.post(base + "/preview", json=wanted.model_dump(mode="json"))
            ).json() == value
            assert (
                await api.post(
                    base + "/start",
                    params={"confirmed": "true"},
                    json=request.model_dump(mode="json"),
                )
            ).json() == value
            assert (files / "tree/nested/nul").stat().st_ino == inode
            assert len((await api.get(base)).json()) == 1
    finally:
        if process.returncode is None:
            process.terminate()
        await asyncio.wait_for(process.communicate(), 15)
