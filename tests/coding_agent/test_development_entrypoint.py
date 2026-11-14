"""Development target through real account/session/approval services and API.

Container and broker IO are injected in this process-level test. The separate
verify_sqlite_terminal.py exercises actual runsc processes and persistent SQLite.
"""

import asyncio
import time
from pathlib import Path

import httpx
import pytest

from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.development.configuration import DevelopmentConfiguration
from krim_agent.development.contracts import ProcessHandle
from krim_agent.development.service import DevelopmentService
from krim_agent.providers.types import ModelText, ModelToolCall
from krim_agent.sandbox.configuration import SandboxConfiguration
from krim_agent.sandbox.filesystem import ProjectFiles
from tests.agent_core.test_account_api import initialize


class ProjectBroker:
    def __init__(self, root: Path):
        self.files = ProjectFiles(root)
        self.root = root

    async def call(self, action, **values):
        if action == "provision_environment":
            return {"status": "ready"}
        if action == "provision_project":
            self.files.create(values["environment_id"], values["project_id"])
            return {"status": "ready"}
        if action == "read":
            return self.files.read(**values)
        if action == "list":
            return self.files.list(**values)
        raise AssertionError("unexpected broker action: " + action)


class ShellModel:
    def __init__(self):
        self.calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        if messages[-1].role == "tool":
            yield ModelText(text="project task finished")
        else:
            assert any(item["name"] == "dev_shell" for item in tools)
            yield ModelToolCall(
                id="shell-" + str(self.calls),
                name="dev_shell",
                arguments={"command": "printf entrypoint"},
            )

    async def close(self):
        pass


@pytest.mark.parametrize("approval", [False, True])
async def test_project_api_and_session_sdk_authorize_the_same_command(
    tmp_path: Path,
    monkeypatch,
    approval: bool,
) -> None:
    path = tmp_path / "application.sqlite"
    token = await initialize(path)
    environments = tmp_path / "environments"
    environments.mkdir(mode=0o700)
    config_path = tmp_path / "sandbox.json"
    config_path.write_text(
        SandboxConfiguration(broker_socket=tmp_path / "fixture.sock").model_dump_json()
    )
    config_path.chmod(0o600)
    configuration = DevelopmentConfiguration(
        image_id="sha256:" + "a" * 64,
        broker_environments_root=environments,
        private_state_root=tmp_path / "private",
    )

    async def supervisor(_self):
        return

    monkeypatch.setattr(DevelopmentService, "start_supervisor", supervisor)
    model = ShellModel()
    app = await open_application(
        path,
        settings=Settings(database=path, sandbox_config_file=config_path),
        provider=model,
        development_configuration=configuration,
    )
    assert app.development is not None and app.sandbox is not None
    development = app.development
    broker = ProjectBroker(environments)
    app.sandbox.broker = broker
    writes = []

    async def start(handle):
        from dataclasses import replace

        record = await development.repositories.environments.resolve(handle)
        ready = replace(
            record,
            revision=record.revision + 1,
            environment=record.environment.model_copy(update={"state": "ready"}),
        )
        await development.repositories.environments.compare_and_swap(record, ready)
        return ready.environment

    async def terminal_submit(request):
        await development.authorize_terminal(request, "submit")
        from krim_agent.development.ports import command_digest

        reservation = await development.repositories.processes.reserve(
            request, command_digest(request)
        )
        assert reservation.execute
        writes.append(request)
        return ProcessHandle(
            **{**reservation.record.handle.model_dump(), "state": "exited", "exit_code": 0}
        )

    async def stop_run(_ownership):
        return

    monkeypatch.setattr(development.environments, "start", start)
    monkeypatch.setattr(development.terminal, "submit", terminal_submit)
    monkeypatch.setattr(development.terminal, "stop_run", stop_run)
    api = create_app(application=app)
    try:
        async with (
            api.router.lifespan_context(api),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=api),
                base_url="http://127.0.0.1",
                headers={"Authorization": "Bearer " + token},
            ) as client,
        ):
            me = (await client.get("/v2/me")).json()
            assert "target-development" in me["target_keys"]
            workspace = me["workspaces"][0]["workspace_id"]
            project = await client.post(
                "/v2/me/sandbox/projects",
                json={
                    "request_key": "create-project",
                    "workspace_id": workspace,
                    "title": "Coding",
                },
            )
            assert project.status_code == 201, project.text
            project_id = project.json()["project_id"]
            prepared = await client.post(
                f"/v2/me/development/projects/{project_id}/prepare", json={"request_key": "prepare"}
            )
            assert prepared.status_code == 200, prepared.text
            assert prepared.json()["state"] == "ready"
            granted = await client.put(
                f"/v2/me/development/projects/{project_id}/grant",
                json={
                    "expected_revision": 0,
                    "expires_at": time.time() + 300,
                    "requires_approval": approval,
                },
            )
            assert granted.status_code == 200, granted.text
            session = await client.post(
                f"/v2/workspaces/{workspace}/sessions",
                json={
                    "request_key": "session",
                    "target_key": "target-development",
                },
            )
            assert session.status_code == 201, session.text
            session_id = session.json()["id"]
            bound = await client.put(
                f"/v2/me/sandbox/sessions/{session_id}/project",
                json={
                    "request_key": "bind",
                    "expected_revision": 0,
                    "expected_session_revision": 0,
                    "project_id": project_id,
                },
            )
            assert bound.status_code == 200, bound.text
            revision = bound.json()["session_revision"]
            turn = await client.post(
                f"/v2/workspaces/{workspace}/sessions/{session_id}/turns",
                json={
                    "request_key": "task",
                    "text": "Run a project task",
                    "expected_revision": revision,
                },
            )
            assert turn.status_code == 202, turn.text
            turn_id = turn.json()["id"]
            actor = app.identities.authenticate("Bearer " + token)
            async with asyncio.timeout(10):
                while True:
                    current = await app.sessions.turn(actor, workspace, session_id, turn_id)
                    if current.run_id is not None:
                        run = await app.service.get(current.run_id)
                        if run.status in {"awaiting", "completed", "failed"}:
                            break
                    await asyncio.sleep(0.01)
            if approval:
                assert run.status == "awaiting", run.code
                assert writes == []
                pending = await app.service.approvals(run.id)
                assert len(pending) == 1 and "project_access" in pending[0]
                from krim_agent.contracts import ApprovalReply

                request = pending[0]
                await app.service.reply_approval(
                    run.id,
                    request["request_id"],
                    ApprovalReply(
                        request_key="allow",
                        call_id=request["call_id"],
                        tool_hash=request["tool_hash"],
                        arguments_hash=request["arguments_hash"],
                        allow=True,
                    ),
                )
                run = await asyncio.wait_for(app.service.wait(run.id), 10)
            assert run.status == "completed", run.code
            assert len(writes) == 1
            assert writes[0].environment.project_id == project_id
            assert writes[0].ownership.session_id == session_id
            assert model.calls == 2
    finally:
        await app.close()
        broker.files.close()
