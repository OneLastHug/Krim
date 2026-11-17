"""target-http formal API uses ordinary schema4 without extending job permits."""

from __future__ import annotations

import asyncio
import json
import time
from types import SimpleNamespace

import httpx
import pytest
from test_account_api import initialize
from test_sandbox_command import IMAGE, CommandBroker

from krim_agent.api.server import create_app
from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.context.types import OrdinarySessionContext
from krim_agent.contracts import ServiceError
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
from krim_agent.runtime.contracts import ExecutionBudget
from krim_agent.sandbox import service as service_module
from krim_agent.sandbox.operations import SandboxOperations
from krim_agent.sandbox.tasks import SandboxCompletion, TaskWait, verify_completion, wait


async def test_target_http_schema4_without_run_timeout_via_formal_api(tmp_path, wire, monkeypatch):
    database = tmp_path / "ordinary-http.sqlite"
    token = await initialize(database)
    broker = CommandBroker()
    monkeypatch.setattr(service_module, "BrokerClient", lambda *a, **k: broker)
    config = tmp_path / "sandbox.json"
    config.write_text(json.dumps({"broker_socket": "/run/krim-test.sock",
                                  "command_image": IMAGE}))
    config.chmod(0o600)
    settings = Settings(database=database, provider="http", http_mode="agent",
                        sandbox_config_file=config, run_timeout=None, tool_timeout=None,
                        max_tools_per_step=1, product_eval_enabled=False)
    command = {}

    def response(body, index):
        if body["messages"][-1]["role"] == "tool":
            delta, finish = {"content": "命令结果已记录"}, "stop"
        else:
            delta = {"tool_calls": [{"index": 0, "id": "schema4-command",
                                      "type": "function", "function": {
                                          "name": "sandbox_command_run",
                                          "arguments": json.dumps(command)}}]}
            finish = "tool_calls"
        frames = ["data: " + json.dumps({"choices": [{"index": 0, "delta": value,
                    "finish_reason": reason}]}) + "\n\n"
                  for value, reason in ((delta, None), ({}, finish))]
        return ("".join(frames) + "data: [DONE]\n\n").encode()

    monkeypatch.setattr(wire, "response", response)
    async with await open_application(
        database, settings=settings, http_config=OpenAICompatibleConfig(
            base_url=f"http://127.0.0.1:{wire.port}/v1", model="fixture",
            max_tool_calls=1, max_request_bytes=262144,
        ),
    ) as app:
        api = create_app(application=app)
        async with api.router.lifespan_context(api), httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://127.0.0.1",
            headers={"Authorization": "Bearer " + token},
        ) as client:
            me = (await client.get("/v2/me")).json()
            workspace = me["workspaces"][0]["workspace_id"]
            project_response = await client.post("/v2/me/sandbox/projects", json={
                "request_key": "project", "workspace_id": workspace, "title": "命令",
            })
            assert project_response.status_code == 201, project_response.text
            project = project_response.json()["project_id"]
            prepared = await client.post(f"/v2/me/sandbox/projects/{project}/provision",
                                         json={"request_key": "prepare"})
            assert prepared.status_code == 200, prepared.text
            command.update(project_id=project, expected_generation=0, program="python",
                           argv=["-c", "print(1)"], wall_seconds=5)
            base = f"/v2/workspaces/{workspace}/sessions"
            created = await client.post(base, json={"request_key": "session",
                                                     "target_key": "target-http"})
            assert created.status_code == 201, created.text
            session_id = created.json()["id"]
            route = base + "/" + session_id
            bound = await client.put(f"/v2/me/sandbox/sessions/{session_id}/project", json={
                "request_key": "bind", "project_id": project, "expected_revision": 0,
                "expected_session_revision": 0,
            })
            assert bound.status_code == 200, bound.text
            submitted = await client.post(route + "/turns", json={
                "request_key": "turn", "text": "执行命令",
                "expected_revision": bound.json()["session_revision"],
            })
            assert submitted.status_code == 202, submitted.text
            turn_id = submitted.json()["id"]
            turn_route = route + "/turns/" + turn_id
            async with asyncio.timeout(60):
                while True:
                    turn = (await client.get(turn_route)).json()
                    if turn["status"] in {"failed", "cancelled", "interrupted"}:
                        pytest.fail(str(turn))
                    approvals = (await client.get(turn_route + "/approvals")).json()
                    if approvals:
                        break
                    await asyncio.sleep(0.05)
            run_id = turn["run_id"]
            snapshot = await app.store.snapshot(run_id)
            assert isinstance(snapshot.session_context, OrdinarySessionContext)
            assert snapshot.session_context.schema_version == 4
            assert app.service.bindings["target-http"].settings.run_timeout is None
            operations = SandboxOperations(app.sandbox)
            budget = ExecutionBudget(None)
            scope = await operations.scope(run_id, lambda: None, budget)
            assert scope.deadline is None
            scope.check()
            finite = await operations.scope(
                run_id, lambda: None, ExecutionBudget(time.monotonic() - 1),
            )
            with pytest.raises(ServiceError, match="permission_denied"):
                finite.check()

            # A forged ordinary owner is rejected before returning a scope.
            original_snapshot = app.store.snapshot
            async def wrong_owner(_run):
                return SimpleNamespace(session_context=snapshot.session_context.model_copy(
                    update={"owner_id": "other-owner"}), events=snapshot.events)
            monkeypatch.setattr(app.store, "snapshot", wrong_owner)
            with pytest.raises(ServiceError, match="permission_denied"):
                await operations.scope(run_id, lambda: None, budget)
            monkeypatch.setattr(app.store, "snapshot", original_snapshot)
            assert broker.starts == []
            pending = approvals[0]
            body = {"request_key": "approve", "allow": True, **{name: pending[name]
                    for name in ("call_id", "tool_hash", "arguments_hash")}}
            approval_url = turn_route + "/approvals/" + pending["request_id"]
            approved = await client.post(approval_url, json=body)
            assert approved.status_code == 200, approved.text
            async with asyncio.timeout(60):
                while True:
                    view = await app.service.get(run_id)
                    if view.status in {"completed", "failed", "aborted", "interrupted"}:
                        break
                    await asyncio.sleep(0.05)
            assert view.status == "completed", view
            assert len(broker.starts) == 1
            assert broker.starts[0]["command"]["wall_seconds"] == 5
            assert (await client.post(approval_url, json=body)).status_code == 200
            assert len(broker.starts) == 1
            result = await client.get(turn_route + "/result")
            assert result.status_code == 200, result.text
            assert (await app.sandbox.project(scope.actor, project))["generation"] == 1


@pytest.mark.parametrize("kind", ["command", "background"])
@pytest.mark.parametrize("expired", [False, True])
async def test_job_wall_is_preserved_without_run_deadline(monkeypatch, kind, expired):
    import krim_agent.sandbox.execution as execution
    import krim_agent.sandbox.tasks as task_module
    from krim_agent.sandbox.command import SandboxCommand
    from krim_agent.sandbox.operations import SessionCommandScope
    from krim_agent.sandbox.tasks import TaskStart
    from krim_agent.tools.core import Invocation
    from krim_agent.tools.pipeline import CURRENT

    service = object()
    operations = SandboxOperations(service)
    calls = []
    async def execute(*args):
        calls.append(args[2].wall_seconds)
        return {}
    monkeypatch.setattr(execution, "execute_command", execute)
    monkeypatch.setattr(task_module, "start_task", execute)
    model = SandboxCommand if kind == "command" else TaskStart
    values = model(expected_generation=0, argv=["-c", "print(1)"], wall_seconds=5)
    invocation = Invocation("run", "call", "span", 0, "sandbox_command_run", "{}", "hash")
    scope = SessionCommandScope(
        actor=object(), workspace_id="workspace", session_id="session",
        operations=operations, check=lambda: None,
        initial_deadline=time.monotonic() - 1 if expired else None,
        invocation=invocation, project_id="project",
    )
    token = CURRENT.set(scope)
    try:
        if expired:
            with pytest.raises(ServiceError, match="run_timeout"):
                await operations.execute(invocation, values, None)
            assert calls == []
        else:
            await operations.execute(invocation, values, None)
            assert calls == [5]
    finally:
        CURRENT.reset(token)


async def test_task_wait_and_completion_accept_no_run_deadline(monkeypatch):
    import krim_agent.sandbox.tasks as tasks

    operation = {"project_id": "project", "operation_id": "operation"}
    async def lookup(*args):
        return operation
    async def observe(*args):
        return "finished", {}, {}
    monkeypatch.setattr(tasks, "_operation", lookup)
    monkeypatch.setattr(tasks, "_observe", observe)
    monkeypatch.setattr(tasks, "_view", lambda *args: {"status": "finished"})
    scope = SimpleNamespace(deadline=None, actor=object(), session_id="session",
                            check=lambda: None)
    values = TaskWait.model_construct(project_id="project", operation_id="operation", seconds=1)
    assert await wait(object(), scope, values, SimpleNamespace(run_id="run")) == {
        "status": "finished"}

    async def no_operations(*args):
        return []
    monkeypatch.setattr(tasks, "_run_operations", no_operations)
    async def record(*args, **kwargs):
        return None
    monkeypatch.setattr(tasks, "_record_verification", record)
    service = SimpleNamespace(store=SimpleNamespace(append=record))
    result = await verify_completion(service, "run", "session", SandboxCompletion(),
                                     deadline=None, check=lambda: None)
    assert result["state"] == "failed"
