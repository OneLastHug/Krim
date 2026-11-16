"""Preview-runner fixture: SQLite sessions, SDK, runsc, HTTP/WS relay and TLS.

The fixed peer is protocol infrastructure, not the user's image-host application.
Project files are real; quota preparation remains a declared fixture.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import shlex
import shutil
import signal
import tempfile
import time
from pathlib import Path

import httpx
import websockets

from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.development.configuration import (
    DevelopmentConfiguration,
    DevelopmentListenerConfiguration,
    DevelopmentPreviewConfiguration,
)
from krim_agent.development.serving import serve_development
from krim_agent.providers.types import ModelText, ModelToolCall
from krim_agent.sandbox.configuration import SandboxConfiguration
from tests.agent_core.test_account_api import initialize
from tests.coding_agent.prepare_preview_listener import certificates, write_private
from tests.coding_agent.verify_development_session import RealFileBroker
from tests.coding_agent.verify_sqlite_terminal import RecordingDocker

PEER = r"""import asyncio, logging, sys
sys.path.insert(0, '/workspace/fixture_lib')
from websockets.asyncio.server import serve
from websockets.datastructures import Headers
from websockets.http11 import Response
logging.disable(logging.CRITICAL)
payload=bytes(range(256))*2000
async def echo(ws):
    async for message in ws:
        await ws.send(message)
def http(connection, request):
    if request.path == '/ws':
        return None
    if request.path not in ('/', '/binary'):
        return Response(404, 'Not Found', Headers([('Content-Length','0')]))
    assert 'authorization' not in request.headers
    assert '__Host-Krim-Preview' not in request.headers.get('cookie','')
    data = payload if request.path == '/binary' else b'preview-service-ok'
    return Response(200, 'OK', Headers([('Content-Type','application/octet-stream'),
        ('Content-Length',str(len(data))),('Connection','close')]), data)
async def main():
    async with serve(echo, '127.0.0.1', 8123, process_request=http, compression=None):
        print('protocol-ready', flush=True)
        await asyncio.Event().wait()
asyncio.run(main())
"""


class ProtocolModel:
    """Deterministic infrastructure fixture using the production SDK tool path."""

    def __init__(self, peer: str = PEER):
        self.peer = peer

    async def stream(self, messages, tools):
        if messages[-1].role != "tool":
            yield ModelToolCall(
                id="start-protocol-service",
                name="dev_shell",
                arguments={
                    "command": "exec /usr/bin/python3 -I -u -c " + shlex.quote(self.peer),
                    "lifecycle": "project_service",
                    "timeout_seconds": None,
                    "wait_seconds": 1.0,
                },
            )
        else:
            yield ModelText(text="Protocol fixture service started.")

    async def close(self):
        pass


def directories(root: Path):
    root.mkdir(mode=0o700, parents=False, exist_ok=False)
    project = Path(tempfile.mkdtemp(prefix="krim-preview-combined-", dir="/srv/krim-sandbox"))
    environments = project / "environments"
    environments.mkdir(mode=0o700)
    certificates(root)
    return project, environments, project.stat()


def stage_library(content: Path):
    # Reuse the already installed, approved library; never execute downloaded code here.
    lib = content / "fixture_lib"
    lib.mkdir(mode=0o755)
    shutil.copytree(
        Path(websockets.__file__).parent,
        lib / "websockets",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.so"),
    )
    for directory in [lib, *(p for p in lib.rglob("*") if p.is_dir())]:
        directory.chmod(0o755)
    for item in lib.rglob("*.py"):
        item.chmod(0o644)


async def main(root: Path, image: str, *, browser: bool = False) -> None:
    project_root, environments, original = await asyncio.to_thread(directories, root)
    browser_module = None
    if browser:
        from tests.coding_agent import browser_combination as browser_module

        await asyncio.to_thread(browser_module.certificates, root)
    preview_port = 443 if browser else 5197
    app = broker = None
    owner = project_id = None
    docker = RecordingDocker(root / "created-container-ids.json")
    try:
        database = root / "application.sqlite"
        token = await initialize(database)
        write_private(root / "test-token", str(token).encode())
        sandbox_file = root / "sandbox.json"
        write_private(
            sandbox_file,
            SandboxConfiguration(
                broker_socket=root / "unused-broker.sock",
            )
            .model_dump_json()
            .encode(),
        )
        configuration = DevelopmentConfiguration(
            image_id=image,
            broker_environments_root=environments,
            private_state_root=root / "private",
            preview=DevelopmentPreviewConfiguration(
                control_origin="https://control.example",
                control_cookie_domain="control.example",
                preview_domain="preview.example",
                listener=DevelopmentListenerConfiguration(
                    control_port=5196,
                    preview_port=preview_port,
                    control_certificate=root / "tls.crt",
                    control_private_key=root / "tls.key",
                    preview_certificate=root / "tls.crt",
                    preview_private_key=root / "tls.key",
                ),
            ),
        )
        app = await open_application(
            database,
            settings=Settings(database=database, sandbox_config_file=sandbox_file),
            provider=ProtocolModel(browser_module.PEER if browser_module is not None else PEER),
            development_configuration=configuration,
        )
        development = app.development
        assert development is not None and app.sandbox is not None and app.identities is not None
        broker = RealFileBroker(environments)
        app.sandbox.broker = broker
        development.environments.runner = docker
        api = create_app(application=app)
        async with (
            api.router.lifespan_context(api),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api),
                base_url="https://control.example",
                headers={"Authorization": "Bearer " + token},
                timeout=60,
            ) as client,
        ):
            me = (await client.get("/v2/me")).json()
            owner, workspace = me["principal_id"], me["workspaces"][0]["workspace_id"]
            response = await client.post(
                "/v2/me/sandbox/projects",
                json={
                    "request_key": "fixture-project",
                    "workspace_id": workspace,
                    "title": "Protocol test",
                },
            )
            assert response.status_code == 201, response.text
            project = response.json()
            project_id = project["project_id"]
            response = await client.post(
                f"/v2/me/development/projects/{project_id}/prepare", json={"request_key": "prepare"}
            )
            assert response.status_code == 200, response.text
            environment = response.json()
            content = environments / project["environment_id"] / "projects" / project_id / "content"
            await asyncio.to_thread(stage_library, content)
            response = await client.put(
                f"/v2/me/development/projects/{project_id}/grant",
                json={
                    "expected_revision": 0,
                    "expires_at": time.time() + 900,
                    "requires_approval": False,
                },
            )
            assert response.status_code == 200, response.text
            grant = response.json()
            response = await client.post(
                f"/v2/workspaces/{workspace}/sessions",
                json={
                    "request_key": "fixture-session",
                    "target_key": "target-development",
                },
            )
            assert response.status_code == 201, response.text
            session_id = response.json()["id"]
            response = await client.put(
                f"/v2/me/sandbox/sessions/{session_id}/project",
                json={
                    "request_key": "bind",
                    "expected_revision": 0,
                    "expected_session_revision": 0,
                    "project_id": project_id,
                },
            )
            assert response.status_code == 200, response.text
            revision = response.json()["session_revision"]
            response = await client.post(
                f"/v2/workspaces/{workspace}/sessions/{session_id}/turns",
                json={
                    "request_key": "fixture-start",
                    "text": "Start the fixed protocol service.",
                    "expected_revision": revision,
                },
            )
            assert response.status_code == 202, response.text
            turn_id = response.json()["id"]
            actor = app.identities.authenticate("Bearer " + token)
            assert app.sessions is not None
            async with asyncio.timeout(60):
                while True:
                    turn = await app.sessions.turn(actor, workspace, session_id, turn_id)
                    if turn.run_id:
                        break
                    await asyncio.sleep(0.05)
                run = await app.service.wait(turn.run_id)
            assert run.status == "completed", run.code
            from krim_agent.development.contracts import EnvironmentHandle

            processes = await development.repositories.processes.list_environment(
                EnvironmentHandle.model_validate(environment),
            )
            assert len(processes) == 1
            process = processes[0].handle
            async with asyncio.timeout(30):
                while True:
                    if (
                        "protocol-ready"
                        in (await development.terminal.read_output(process, 0)).text
                    ):
                        break
                    process = await development.terminal.inspect(process)
                    if process.state not in {"accepted", "running"}:
                        raise RuntimeError("protocol service failed")
                    await asyncio.sleep(0.1)
            write_private(
                root / "binding.json",
                json.dumps(
                    {
                        "project_id": project_id,
                        "process_id": process.process_id,
                        "port": 8123,
                        "control_port": 5196,
                        "preview_port": preview_port,
                        "grant_revision": grant["revision"],
                        "grant_expires": grant["expires_at"],
                        "owner_id": owner,
                        "run_id": run.id,
                        "run_status": run.status,
                        "model": "deterministic protocol fixture",
                        "provisioning": "real ProjectFiles, quota fixture",
                    }
                ).encode(),
            )
        stop = asyncio.Event()

        async def stop_watch():
            deadline = asyncio.get_running_loop().time() + 600
            while not stop.is_set():
                if await asyncio.to_thread((root / "stop").exists):
                    stop.set()
                    return
                if asyncio.get_running_loop().time() >= deadline:
                    stop.set()
                    return
                await asyncio.sleep(0.2)

        watcher = asyncio.create_task(stop_watch())
        browser_task = None
        if browser_module is not None:
            browser_task = asyncio.create_task(browser_module.verify(
                app, root, process, grant, actor, stop,
            ))
        try:
            await serve_development(app, stop=stop)
        finally:
            stop.set()
            await watcher
            if browser_task is not None:
                await browser_task
    finally:
        if app is not None:
            await app.close()
        cleanup = []
        for cid in docker.created:
            reply = await docker.run(("inspect", cid))
            if reply.code:
                cleanup.append({"id": cid, "absent": True})
                continue
            value = json.loads(reply.stdout)[0]
            labels = value["Config"].get("Labels", {})
            assert value["Id"] == cid and value["Image"] == image
            assert labels.get("io.krim.development.owner") == owner
            assert labels.get("io.krim.development.project") == project_id
            assert (await docker.run(("rm", "--force", cid))).code == 0
            assert (await docker.run(("inspect", cid))).code != 0
            cleanup.append({"id": cid, "removed": True})
        worker = app.development._worker_process if app is not None and app.development else None
        if worker is not None and worker.returncode is None:
            worker.send_signal(signal.SIGTERM)
            try:
                await asyncio.wait_for(worker.wait(), 5)
            except TimeoutError:
                worker.kill()
                await worker.wait()
        if broker is not None:
            broker.files.close()
        current = await asyncio.to_thread(project_root.lstat)
        assert (current.st_dev, current.st_ino) == (original.st_dev, original.st_ino)
        await asyncio.to_thread(shutil.rmtree, project_root)
        write_private(
            root / "cleanup.json",
            json.dumps(
                {
                    "containers": cleanup,
                    "project_removed": True,
                    "supervisor_exit": worker.returncode if worker is not None else None,
                }
            ).encode(),
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--browser", action="store_true")
    args = parser.parse_args()
    asyncio.run(main(args.evidence, args.image, browser=args.browser))
