"""工作区修改的REST、同步客户端和CLI公开消费端。"""

import asyncio
import hashlib
import json
import os
import socket
import subprocess
import sys
from pathlib import Path
from threading import Event

import httpx
import pytest
import uvicorn
from pydantic import SecretStr

from krim_agent.api import create_app
from krim_agent.client import ClientError, KrimClient
from krim_agent.providers import FakeProvider
from krim_agent.workspace_change import (
    FileChangeSpec,
    WorkspaceChangeAction,
    WorkspacePreviewRequest,
)
from krim_agent.workspace_writer import WorkspaceWriter

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="需要Linux条件文件事务"
)


async def allow_or_ask(definition, _context):
    return "allow" if definition.risk == "read" else "ask"


async def cli(url, *args):
    env = {key: value for key, value in os.environ.items() if not key.startswith("KRIM_AGENT_")}
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "krim_agent",
        "workspace",
        *map(str, args),
        "--url",
        url,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
    return process.returncode, stdout.decode(), stderr.decode()


async def test_rest_change_lifecycle_has_no_root_or_recovery_escape(runtime, tmp_path):
    file = tmp_path / "note.txt"
    file.write_text("old\n", encoding="utf-8")
    runtime.tool_policy = allow_or_ask
    app = create_app(runtime=runtime, workspace_root=tmp_path, workspace_writable=True)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api,
    ):
        agent = (await api.post("/v1/agents", json={"name": "default"})).json()
        agent_id = agent["agent_id"]
        old = (
            await api.get(f"/v1/agents/{agent_id}/workspace/read", params={"path": "note.txt"})
        ).json()
        preview_payload = {
            "spec": {
                "path": "note.txt",
                "operation": "write",
                "expected_sha256": old["sha256"],
                "content": "new\n",
            },
            "request_key": "rest-preview",
        }
        preview = await api.post(
            f"/v1/agents/{agent_id}/workspace/changes/preview", json=preview_payload
        )
        assert preview.status_code == 200
        proposal = preview.json()
        assert proposal["status"] == "proposed" and "root" not in json.dumps(proposal)
        action = await api.post(
            f"/v1/agents/{agent_id}/workspace/changes?confirmed=true",
            json={
                "proposal_id": proposal["proposal_id"],
                "action": "apply",
                "expected_revision": proposal["revision"],
                "request_key": "rest-apply",
            },
        )
        assert action.status_code == 200 and action.json()["status"] == "done"
        operation_id = action.json()["operation_id"]
        assert file.read_text() == "new\n"
        assert (await api.get(f"/v1/agents/{agent_id}/workspace/changes")).json()[0][
            "status"
        ] == "applied"
        history = await api.get(
            f"/v1/agents/{agent_id}/workspace/changes/{proposal['proposal_id']}/history"
        )
        assert history.status_code == 200 and history.json()[0]["operation_id"] == operation_id
        operation = await api.get(f"/v1/agents/{agent_id}/workspace/operations/{operation_id}")
        assert operation.json()["cleanup_pending"] is True
        mismatch = await api.post(
            f"/v1/agents/{agent_id}/workspace/operations/{operation_id}/finalize",
            json={"operation_id": "other"},
            params={"confirmed": "true"},
        )
        assert mismatch.status_code == 422
        finalized = await api.post(
            f"/v1/agents/{agent_id}/workspace/operations/{operation_id}/finalize",
            params={"confirmed": "true"},
        )
        assert finalized.status_code == 200 and finalized.json()["cleanup_pending"] is False
        forged = await api.post(
            f"/v1/agents/{agent_id}/workspace/changes/preview",
            json={
                "spec": {
                    "path": "note.txt",
                    "operation": "write",
                    "expected_sha256": old["sha256"],
                    "content": "other\n",
                    "root": "/tmp",
                },
                "request_key": "forged",
            },
        )
        assert forged.status_code == 422
        assert file.read_text() == "new\n"


async def test_real_tcp_sync_client_and_cli_manage_same_change(tmp_path, monkeypatch):
    root = tmp_path / "files"
    root.mkdir()
    file = root / "note.txt"
    file.write_text("old\n", encoding="utf-8")
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    port = listener.getsockname()[1]
    from krim_agent.config import Settings
    from krim_agent.persistence import Database, KrimRepository
    from krim_agent.runtime import ChatRuntime
    from krim_agent.tools.builtins import builtin_registry

    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'api.sqlite'}")
    await database.initialize()
    runtime = ChatRuntime(
        KrimRepository(database), FakeProvider(delay=0), builtin_registry(), Settings()
    )
    runtime.tool_policy = allow_or_ask
    app = create_app(runtime=runtime, workspace_root=root, workspace_writable=True)
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    server_task = asyncio.create_task(server.serve(sockets=[listener]))
    url = f"http://127.0.0.1:{port}"
    try:
        async with asyncio.timeout(10):
            while not server.started:
                if server_task.done():
                    await server_task
                await asyncio.sleep(0.01)

        def client_flow() -> tuple[str, str, str]:
            with KrimClient(url) as client:
                agent = client.agent_spawn("default")
                old = client.agent_read_file(agent.agent_id, "note.txt")
                proposal = client.agent_workspace_preview(
                    agent.agent_id,
                    WorkspacePreviewRequest(
                        spec=FileChangeSpec(
                            path="note.txt",
                            operation="write",
                            expected_sha256=old.sha256,
                            content="client\n",
                        ),
                        request_key="client-preview",
                    ),
                )
                result = client.agent_workspace_change(
                    agent.agent_id,
                    WorkspaceChangeAction(
                        proposal_id=proposal.proposal_id,
                        action="apply",
                        expected_revision=proposal.revision,
                        request_key="client-apply",
                    ),
                    confirmed=True,
                )
                assert result.status == "done"
                return agent.agent_id, proposal.proposal_id, result.operation_id

        agent_id, proposal_id, operation_id = await asyncio.to_thread(client_flow)
        assert file.read_text() == "client\n"
        request_path = tmp_path / "preview.json"
        request_path.write_text(
            json.dumps(
                {
                    "spec": {
                        "path": "note.txt",
                        "operation": "write",
                        "expected_sha256": hashlib.sha256(b"client\n").hexdigest(),
                        "content": "cli\n",
                    },
                    "request_key": "cli-preview",
                }
            )
        )
        for command in [
            ["changes", agent_id],
            ["operation", agent_id, operation_id],
            ["history", agent_id, proposal_id],
        ]:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "krim_agent",
                "workspace",
                *command,
                "--url",
                url,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), 10)
            assert process.returncode == 0, stderr.decode()
            assert json.loads(stdout)
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "krim_agent",
            "workspace",
            "preview",
            agent_id,
            "--request",
            str(request_path),
            "--url",
            url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), 10)
        assert process.returncode == 0, stderr.decode()
        proposed = json.loads(stdout)
        assert proposed["status"] == "proposed"
        proposal_id = proposed["proposal_id"]
        code, stdout, stderr = await cli(
            url,
            "apply",
            agent_id,
            proposal_id,
            "--revision",
            proposed["revision"],
            "--request-key",
            "cli-apply",
        )
        assert code == 1 and "approval_required" in stderr
        code, stdout, stderr = await cli(
            url,
            "apply",
            agent_id,
            proposal_id,
            "--revision",
            proposed["revision"],
            "--request-key",
            "cli-apply",
            "--confirm",
        )
        assert code == 0, stderr
        applied = json.loads(stdout)
        assert applied["status"] == "done" and file.read_text() == "cli\n"
        code, stdout, stderr = await cli(
            url,
            "apply",
            agent_id,
            proposal_id,
            "--revision",
            proposed["revision"],
            "--request-key",
            "cli-apply",
            "--confirm",
        )
        assert code == 0 and json.loads(stdout)["operation_id"] == applied["operation_id"]
        for operation in ["reconcile", "finalize"]:
            code, stdout, stderr = await cli(
                url, operation, agent_id, applied["operation_id"], "--confirm"
            )
            assert code == 0, stderr
            assert json.loads(stdout)["status"] == "done"
        for operation in ["keep", "undo"]:
            code, stdout, stderr = await cli(url, "proposal", agent_id, proposal_id)
            assert code == 0, stderr
            current = json.loads(stdout)
            code, stdout, stderr = await cli(
                url,
                operation,
                agent_id,
                proposal_id,
                "--revision",
                current["revision"],
                "--request-key",
                f"cli-{operation}",
                "--confirm",
            )
            assert code == 0, stderr
            result = json.loads(stdout)
            assert result["status"] == "done"
            code, stdout, stderr = await cli(
                url, "finalize", agent_id, result["operation_id"], "--confirm"
            )
            assert code == 0, stderr
        assert file.read_text() == "client\n"
        code, stdout, stderr = await cli(url, "changes", agent_id, "--offset", 0, "--limit", 1)
        assert code == 0 and len(json.loads(stdout)) == 1

        store = app.state.agents.workspace_edits
        original_settle = store.settle
        failed = False

        async def save_fault(*args, **kwargs):
            nonlocal failed
            if not failed:
                failed = True
                raise OSError("injected receipt loss")
            return await original_settle(*args, **kwargs)

        monkeypatch.setattr(store, "settle", save_fault)
        async with httpx.AsyncClient(base_url=url) as remote:
            proposal = (
                await remote.post(
                    f"/v1/agents/{agent_id}/workspace/changes/preview",
                    json={
                        "spec": {
                            "path": "note.txt",
                            "operation": "write",
                            "expected_sha256": hashlib.sha256(b"client\n").hexdigest(),
                            "content": "unknown\n",
                        },
                        "request_key": "fault-preview",
                    },
                )
            ).json()
        code, stdout, stderr = await cli(
            url,
            "apply",
            agent_id,
            proposal["proposal_id"],
            "--revision",
            proposal["revision"],
            "--request-key",
            "fault-apply",
            "--confirm",
        )
        assert code == 1 and json.loads(stdout)["status"] == "uncertain"
        unknown_id = json.loads(stdout)["operation_id"]
        code, stdout, stderr = await cli(url, "reconcile", agent_id, unknown_id, "--confirm")
        assert code == 0 and json.loads(stdout)["status"] == "done"
        code, stdout, stderr = await cli(url, "finalize", agent_id, unknown_id, "--confirm")
        assert code == 0 and not json.loads(stdout)["cleanup_pending"]
        monkeypatch.setattr(store, "settle", original_settle)

        entered, release = Event(), Event()
        original_apply = WorkspaceWriter.apply
        publications = 0

        def blocked(self, *args, **kwargs):
            nonlocal publications
            publications += 1
            entered.set()
            assert release.wait(8)
            return original_apply(self, *args, **kwargs)

        monkeypatch.setattr(WorkspaceWriter, "apply", blocked)
        try:
            async with httpx.AsyncClient(base_url=url) as remote:
                proposal = (
                    await remote.post(
                        f"/v1/agents/{agent_id}/workspace/changes/preview",
                        json={
                            "spec": {
                                "path": "note.txt",
                                "operation": "write",
                                "expected_sha256": hashlib.sha256(b"unknown\n").hexdigest(),
                                "content": "disconnected\n",
                            },
                            "request_key": "disconnect-preview",
                        },
                    )
                ).json()
                body = json.dumps(
                    {
                        "proposal_id": proposal["proposal_id"],
                        "action": "apply",
                        "expected_revision": proposal["revision"],
                        "request_key": "disconnect-apply",
                    }
                ).encode()
                _reader, writer = await asyncio.open_connection("127.0.0.1", port)
                writer.write(
                    (
                        f"POST /v1/agents/{agent_id}/workspace/changes?confirmed=true HTTP/1.1\r\n"
                        f"Host: 127.0.0.1:{port}\r\nContent-Type: application/json\r\n"
                        f"Content-Length: {len(body)}\r\n\r\n"
                    ).encode()
                    + body
                )
                await writer.drain()
                assert await asyncio.to_thread(entered.wait, 5)
                writer.close()
                await writer.wait_closed()
                release.set()
                async with asyncio.timeout(8):
                    while True:
                        history = (
                            await remote.get(
                                f"/v1/agents/{agent_id}/workspace/changes/{proposal['proposal_id']}/history"
                            )
                        ).json()
                        if history and history[0]["status"] == "done":
                            break
                        await asyncio.sleep(0.01)
                assert publications == 1 and file.read_text() == "disconnected\n"
        finally:
            release.set()
    finally:
        server.should_exit = True
        await asyncio.wait_for(server_task, 10)
        listener.close()
        await runtime.close()
        await database.close()


@pytest.mark.parametrize(
    "boundary", ["bearer", "origin", "deny", "readonly", "subset", "cross-session"]
)
async def test_rest_write_boundaries_do_not_grant_unapproved_capability(
    runtime, tmp_path, boundary
):
    (tmp_path / "note.txt").write_text("old\n")
    runtime.tool_policy = None if boundary == "deny" else allow_or_ask
    headers = {}
    if boundary == "bearer":
        runtime.settings = runtime.settings.model_copy(
            update={"api_key": SecretStr("test-workspace-key")}
        )
        headers = {"Authorization": "Bearer test-workspace-key"}
    app = create_app(
        runtime=runtime, workspace_root=tmp_path, workspace_writable=boundary != "readonly"
    )
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api,
    ):
        agent = (await api.post("/v1/agents", json={"name": "default"}, headers=headers)).json()
        identifier = agent["agent_id"]
        preview_body = {
            "spec": {
                "path": "note.txt",
                "operation": "write",
                "expected_sha256": hashlib.sha256(b"old\n").hexdigest(),
                "content": "new\n",
            },
            "request_key": "preview",
        }
        if boundary == "readonly":
            rejected = await api.post(
                f"/v1/agents/{identifier}/workspace/changes/preview", json=preview_body
            )
            assert (
                rejected.status_code == 403 and rejected.json()["error"]["code"] == "tool_not_found"
            )
            return
        proposal = (
            await api.post(
                f"/v1/agents/{identifier}/workspace/changes/preview",
                json=preview_body,
                headers=headers,
            )
        ).json()
        action_headers = headers
        if boundary == "bearer":
            action_headers = {}
        elif boundary == "origin":
            action_headers = {"Origin": "https://untrusted.example"}
        elif boundary == "subset":
            await api.put(
                "/v1/agents/profiles/reader",
                json={
                    "expected_revision": 0,
                    "config": {
                        "name": "reader",
                        "description": "只读工具子集",
                        "workspace_key": "local",
                        "tool_names": ["workspace_read"],
                    },
                },
            )
            identifier = (await api.post("/v1/agents", json={"name": "reader"})).json()["agent_id"]
        elif boundary == "cross-session":
            identifier = (await api.post("/v1/agents", json={"name": "default"})).json()["agent_id"]
        response = await api.post(
            f"/v1/agents/{identifier}/workspace/changes",
            headers=action_headers,
            params={"confirmed": "true"},
            json={
                "proposal_id": proposal["proposal_id"],
                "action": "apply",
                "expected_revision": 0,
                "request_key": "apply",
            },
        )
        assert (
            response.status_code
            == {"bearer": 401, "origin": 403, "deny": 403, "subset": 403, "cross-session": 404}[
                boundary
            ]
        )
        assert (tmp_path / "note.txt").read_text() == "old\n"


@pytest.mark.parametrize(
    "parameter", [{"offset": -1}, {"limit": 0}, {"limit": 101}, {"offset": 2**64}]
)
async def test_rest_change_pagination_is_bounded(runtime, tmp_path, parameter):
    app = create_app(runtime=runtime, workspace_root=tmp_path, workspace_writable=True)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api,
    ):
        agent = (await api.post("/v1/agents", json={"name": "default"})).json()
        response = await api.get(
            f"/v1/agents/{agent['agent_id']}/workspace/changes", params=parameter
        )
        assert response.status_code == 422


async def test_closed_session_history_remains_readable_and_paths_and_bodies_are_strict(
    runtime, tmp_path
):
    runtime.tool_policy = allow_or_ask
    app = create_app(runtime=runtime, workspace_root=tmp_path, workspace_writable=True)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1"
        ) as api,
    ):
        agent = (await api.post("/v1/agents", json={"name": "default"})).json()
        base = f"/v1/agents/{agent['agent_id']}/workspace"
        body = {
            "spec": {
                "path": "created.txt",
                "operation": "write",
                "expected_sha256": None,
                "content": "new",
            },
            "request_key": "preview",
        }
        preview = (await api.post(f"{base}/changes/preview", json=body)).json()
        assert (await api.post(f"{base}/changes/preview", content=b"x" * 65537)).status_code == 413
        assert (await api.get(f"{base}/operations/" + "a" * 37)).status_code == 422
        assert (
            await api.post(
                f"{base}/operations/unknown/reconcile",
                params={"confirmed": "true"},
                json={"root": "/tmp"},
            )
        ).status_code == 422
        assert (await api.get(f"{base}/changes/unknown")).status_code == 404
        session = (await api.get(f"/v1/agents/{agent['agent_id']}/session")).json()
        await api.post(
            f"/v1/agents/{agent['agent_id']}/session/close",
            json={"expected_revision": session["revision"]},
        )
        assert (await api.get(f"{base}/changes/{preview['proposal_id']}")).status_code == 200
        assert (await api.get(f"{base}/changes/{preview['proposal_id']}/history")).json() == []
        rejected = await api.post(
            f"{base}/changes",
            params={"confirmed": "true"},
            json={
                "proposal_id": preview["proposal_id"],
                "action": "apply",
                "expected_revision": 0,
                "request_key": "apply",
            },
        )
        assert rejected.status_code == 410 and not (tmp_path / "created.txt").exists()


@pytest.mark.parametrize(
    "identifier", ["../agent/destroy", "a?confirmed=true", "x#fragment", "a" * 37]
)
def test_client_rejects_path_identifiers_without_sending_requests(identifier):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json={})

    with KrimClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(ClientError) as error:
            client.agent_workspace_finalize("agent", identifier, confirmed=True)
        assert error.value.code == "workspace_identifier_invalid"
    assert not calls


def test_client_network_failure_does_not_retry_file_action():
    calls = 0

    def broken(_request):
        nonlocal calls
        calls += 1
        raise httpx.ReadTimeout("response lost after request acceptance")

    with KrimClient(transport=httpx.MockTransport(broken)) as client:
        with pytest.raises(httpx.ReadTimeout):
            client.agent_workspace_change(
                "agent",
                WorkspaceChangeAction(
                    proposal_id="proposal",
                    action="apply",
                    expected_revision=0,
                    request_key="fixed-key",
                ),
                confirmed=True,
            )
    assert calls == 1


@pytest.mark.parametrize("case", ["missing", "invalid", "oversized", "extra-root"])
def test_cli_invalid_preview_is_bounded_and_does_not_echo_private_input(tmp_path, case):
    source = tmp_path / "request.json"
    if case == "invalid":
        source.write_text('{"private":"PRIVATE-INPUT-CANARY"')
    elif case == "oversized":
        source.write_text("PRIVATE-INPUT-CANARY" + "x" * 65537)
    elif case == "extra-root":
        source.write_text(
            json.dumps(
                {
                    "spec": {
                        "path": "x",
                        "operation": "write",
                        "expected_sha256": None,
                        "content": "PRIVATE-INPUT-CANARY",
                        "root": "/tmp",
                    },
                    "request_key": "bad",
                }
            )
        )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "krim_agent",
            "workspace",
            "preview",
            "agent",
            "--request",
            str(source),
            "--url",
            "http://127.0.0.1:1",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 2
    assert "PRIVATE-INPUT-CANARY" not in result.stdout + result.stderr


async def test_management_api_restart_in_independent_process_reads_original_operation(tmp_path):
    root = tmp_path / "files"
    root.mkdir()
    (root / "file.txt").write_text("old\n")
    script = str(Path(__file__).parent / "fixtures" / "workspace_management_worker.py")
    outputs = []
    for mode in ("create", "restore"):
        child = await asyncio.create_subprocess_exec(
            sys.executable,
            script,
            str(tmp_path / "runs.sqlite"),
            str(root),
            mode,
            str(tmp_path / "metadata.json"),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(child.communicate(), 60)
        assert child.returncode == 0, stderr.decode()
        outputs.append(json.loads(stdout.decode().splitlines()[-1]))
    assert outputs[0]["agent_id"] == outputs[1]["agent_id"]
    assert outputs[0]["operation_id"] == outputs[1]["operation_id"]
    assert (root / "file.txt").read_text() == "old\n"
