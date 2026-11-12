"""新命令走真实AgentLoop审批停靠；合成Broker不冒充runsc环境验收。"""

import asyncio
import base64
import json
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import update
from test_account_api import initialize

from krim_agent.composition import open_application
from krim_agent.config import Settings
from krim_agent.contracts import ApprovalReply, ServiceError
from krim_agent.providers.openai_compatible import OpenAICompatibleConfig
from krim_agent.sandbox import service as service_module
from krim_agent.sessions.types import SessionCreate, TurnRequest
from krim_agent.storage.sandbox_models import sandbox_projects

IMAGE = "sha256:" + "d" * 64


class CommandBroker:
    def __init__(self):
        self.starts = []
        self.polls = 0
        self.cancelled = False
        self.waiting = False
        self.entered = asyncio.Event()

    async def call(self, action, **arguments):
        if action == "health":
            return {
                "storage_available": True,
                "execution_available": True,
                "image": IMAGE,
                "available_programs": ["python"],
                "runtimes": {
                    "python": {"image": IMAGE, "fingerprint": None, "image_id": IMAGE}
                },
            }
        if action in {"command_start", "file_start"}:
            self.starts.append(arguments)
            return {"status": "accepted"}
        if action == "command_status":
            self.polls += 1
            self.entered.set()
            self.cancelled |= arguments["cancel"]
            if self.waiting and not self.cancelled:
                return {"status": "started", "executor_started": True}
            return {
                "status": "finished",
                "generation": 1,
                "receipt": {
                    "executor_started": True,
                    "exit_code": 143 if self.cancelled else 0,
                    "cleanup": "clean",
                    "output_complete": True,
                    "stdout": base64.b64encode(b"approved output").decode(),
                    "stderr": "",
                    "code": "cancelled" if self.cancelled else None,
                },
            }
        return {"status": "ready"}


@asynccontextmanager
async def command_app(
    tmp_path,
    wire,
    monkeypatch,
    *,
    real_broker=None,
    image=IMAGE,
    command_argv=None,
    tool_name="sandbox_command_run",
    tool_body=None,
    bind_session=False,
    omit_project=False,
    park=True,
):
    database = tmp_path / "app.sqlite"
    token = await initialize(database)
    broker = real_broker if real_broker is not None else CommandBroker()
    monkeypatch.setattr(service_module, "BrokerClient", lambda *a, **k: broker)
    configuration = tmp_path / "sandbox.json"
    configuration.write_text(
        json.dumps({"broker_socket": "/run/krim-test.sock", "command_image": image})
    )
    configuration.chmod(0o600)
    settings = Settings(
        database=database,
        provider="http",
        http_mode="agent",
        max_tools_per_step=1,
        sandbox_config_file=configuration,
        product_eval_enabled=False,
    )
    async with await open_application(
        database,
        settings=settings,
        http_config=OpenAICompatibleConfig(
            base_url=f"http://127.0.0.1:{wire.port}/v1", model="fixture", max_tool_calls=1
        ),
    ) as app:
        actor = app.identities.authenticate("Bearer " + token)
        workspace = actor.memberships[0].workspace_id
        project = await app.sandbox.create_project(
            actor, {"workspace_id": workspace, "title": "命令项目"}, request_key="project"
        )
        await app.sandbox.provision(actor, project_id=project["project_id"], request_key="prepare")
        command = {
            "project_id": project["project_id"],
            "expected_generation": 0,
            "argv": command_argv or ["-c", "print('approved output')"],
            "wall_seconds": 5,
        }

        if tool_body is not None:
            command = {"project_id": project["project_id"], **tool_body}
        if omit_project:
            command.pop("project_id", None)

        def response(body, index):
            if body["messages"][-1]["role"] == "tool":
                delta, finish = {"content": "命令结果已记录"}, "stop"
            else:
                delta = {
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": f"command-{index}",
                            "type": "function",
                            "function": {
                                "name": tool_name,
                                "arguments": json.dumps(command),
                            },
                        }
                    ]
                }
                finish = "tool_calls"

            def frame(d, reason):
                return (
                    "data: "
                    + json.dumps({"choices": [{"index": 0, "delta": d, "finish_reason": reason}]})
                    + "\n\n"
                )

            return (frame(delta, None) + frame({}, finish) + "data: [DONE]\n\n").encode()

        monkeypatch.setattr(wire, "response", response)
        session = await app.sessions.create(
            actor, workspace, SessionCreate(request_key="session", target_key="target-http")
        )
        session_revision = 0
        if bind_session:
            from krim_agent.sandbox.bindings import bind_project

            selected = await bind_project(
                app.sandbox,
                actor,
                session.id,
                {
                    "project_id": project["project_id"],
                    "expected_revision": 0,
                    "expected_session_revision": 0,
                },
                request_key="bind-session",
            )
            session_revision = selected["session_revision"]
        turn = await app.sessions.submit(
            actor,
            workspace,
            session.id,
            TurnRequest(request_key="turn", text="执行命令", expected_revision=session_revision),
        )
        async with asyncio.timeout(10):
            while True:
                current = await app.sessions.turn(actor, workspace, session.id, turn.id)
                if current.run_id:
                    view = await app.service.get(current.run_id)
                    if not park and view.status == "completed":
                        break
                    if view.status == "awaiting":
                        break
                    if view.status in {"failed", "completed", "aborted"}:
                        events = await app.service.events(current.run_id)
                        raise AssertionError((view, events))
                await asyncio.sleep(0.01)
        approvals = await app.service.approvals(current.run_id)
        yield app, broker, actor, project, current.run_id, approvals[0] if approvals else None


def approval(request, allow=True):
    return ApprovalReply(
        request_key="approval",
        call_id=request["call_id"],
        tool_hash=request["tool_hash"],
        arguments_hash=request["arguments_hash"],
        allow=allow,
    )


async def finished(app, run_id):
    async with asyncio.timeout(10):
        while True:
            view = await app.service.get(run_id)
            if view.status not in {"running", "queued", "awaiting"}:
                return view
            await asyncio.sleep(0.01)


async def test_command_parks_then_executes_once_after_approval(tmp_path, wire, monkeypatch):
    async with command_app(tmp_path, wire, monkeypatch) as (
        app,
        broker,
        actor,
        project,
        run,
        request,
    ):
        assert broker.starts == []
        assert (await app.sandbox.project(actor, project["project_id"]))["generation"] == 0
        await app.service.reply_approval(run, request["request_id"], approval(request))
        view = await finished(app, run)
        assert view.status == "completed", view
        assert len(broker.starts) == 1
        events = await app.service.events(run)
        ends = [e.data for e in events if e.kind == "tool_end"]
        assert ends[0]["decision"] == "ask"
        receipt = ends[0]["value"]
        assert receipt["verification"] == "not_evaluated"
        assert receipt["generation"] == 1
        assert "artifact_revision_id" not in receipt
        assert base64.b64decode(receipt["stdout"]) == b"approved output"
        assert (await app.sandbox.project(actor, project["project_id"]))["status"] == "ready"
        await app.service.reply_approval(run, request["request_id"], approval(request))
        assert len(broker.starts) == 1


async def test_rejected_command_never_reaches_broker(tmp_path, wire, monkeypatch):
    async with command_app(tmp_path, wire, monkeypatch) as (
        app,
        broker,
        actor,
        project,
        run,
        request,
    ):
        await app.service.reply_approval(run, request["request_id"], approval(request, False))
        await finished(app, run)
        assert broker.starts == []
        assert (await app.sandbox.project(actor, project["project_id"]))["generation"] == 0


async def test_stale_project_generation_is_rechecked_after_approval(tmp_path, wire, monkeypatch):
    async with command_app(tmp_path, wire, monkeypatch) as (app, broker, _, project, run, request):
        await app.store._write(
            lambda conn: conn.execute(
                update(sandbox_projects)
                .where(sandbox_projects.c.project_id == project["project_id"])
                .values(generation=1)
            )
        )
        await app.service.reply_approval(run, request["request_id"], approval(request))
        await finished(app, run)
        assert broker.starts == []
        ends = [e.data for e in await app.service.events(run) if e.kind == "tool_end"]
        assert ends[0]["code"] == "input_revision_conflict"


async def test_cancel_waits_for_broker_stop_and_keeps_generation(tmp_path, wire, monkeypatch):
    async with command_app(tmp_path, wire, monkeypatch) as (
        app,
        broker,
        actor,
        project,
        run,
        request,
    ):
        broker.waiting = True
        await app.service.reply_approval(run, request["request_id"], approval(request))
        async with asyncio.timeout(5):
            await broker.entered.wait()
        await app.service.cancel(run)
        await finished(app, run)
        assert broker.cancelled
        saved = await app.sandbox.project(actor, project["project_id"])
        assert saved["generation"] == 1 and saved["status"] == "ready"


async def test_direct_run_cannot_access_session_command_tool(tmp_path, wire, monkeypatch):
    from krim_agent.contracts import RunRequest

    async with command_app(tmp_path, wire, monkeypatch) as (app, broker, _, _, _, _):
        with pytest.raises(ServiceError, match="workspace_contract_required"):
            await app.service.start(
                RunRequest(text="direct command", request_key="direct", target="target-http")
            )
        assert broker.starts == []


async def test_permission_generation_change_invalidates_parked_approval(
    tmp_path, wire, monkeypatch
):
    async with command_app(tmp_path, wire, monkeypatch) as (
        app,
        broker,
        actor,
        project,
        run,
        request,
    ):
        current = await app.identities.account(actor, actor.principal_id)
        # 创建另一管理员后可降级自身，触发generation变化；停靠原授权不能复用。
        other = await app.identities.create_account(
            actor,
            {"display_name": "另一个管理员", "platform_role": "platform_admin"},
            request_key="other-admin",
        )
        await app.identities.issue_key(
            actor, other.account.principal_id, {}, request_key="other-key"
        )
        await app.identities.update_account(
            actor,
            actor.principal_id,
            {"expected_revision": current.revision, "platform_role": "member"},
            request_key="demote",
        )
        try:
            await app.service.reply_approval(run, request["request_id"], approval(request))
        except ServiceError:
            pass
        await finished(app, run)
        assert broker.starts == []


def test_supervisor_and_command_inputs_are_bounded():
    import uuid

    from pydantic import ValidationError

    from krim_agent.sandbox.command import SandboxCommand
    from krim_agent.sandbox.runner import GATE

    compile(GATE, "trusted-supervisor", "exec")
    good = {"project_id": str(uuid.uuid4()), "expected_generation": 0, "argv": ["-c", "print(1)"]}
    assert SandboxCommand.model_validate(good).program == "python"
    for patch in [
        {"argv": []},
        {"argv": ["x" + chr(0)]},
        {"wall_seconds": True},
        {"program": "shell"},
        {"project_id": "../../outside"},
        {"expected_generation": True},
        {"host_path": "/"},
    ]:
        with pytest.raises((ValidationError, ServiceError)):
            SandboxCommand.model_validate({**good, **patch})


async def test_health_failure_before_submission_does_not_block_project(tmp_path, wire, monkeypatch):
    async with command_app(tmp_path, wire, monkeypatch) as (
        app,
        broker,
        actor,
        project,
        run,
        request,
    ):
        original = broker.call

        async def unavailable(action, **arguments):
            if action == "health":
                raise ServiceError("sandbox_backend_unavailable", 503)
            return await original(action, **arguments)

        broker.call = unavailable
        await app.service.reply_approval(run, request["request_id"], approval(request))
        await finished(app, run)
        saved = await app.sandbox.project(actor, project["project_id"])
        assert saved["status"] == "ready" and saved["generation"] == 0
        assert broker.starts == []
        ends = [e.data for e in await app.service.events(run) if e.kind == "tool_end"]
        assert ends[0]["code"] == "sandbox_backend_unavailable"


async def test_capacity_rejection_before_acceptance_leaves_project_writable(
    tmp_path, wire, monkeypatch
):
    async with command_app(tmp_path, wire, monkeypatch) as (
        app,
        broker,
        actor,
        project,
        run,
        request,
    ):
        original = broker.call

        async def capacity(action, **arguments):
            if action == "command_start":
                raise ServiceError("capacity", 429)
            return await original(action, **arguments)

        broker.call = capacity
        await app.service.reply_approval(run, request["request_id"], approval(request))
        await finished(app, run)
        saved = await app.sandbox.project(actor, project["project_id"])
        assert saved["status"] == "ready" and saved["generation"] == 0
        assert saved.get("lease_id") is None
        assert broker.starts == []


async def test_provisioning_cannot_clear_a_running_or_blocked_command(tmp_path, wire, monkeypatch):
    async with command_app(tmp_path, wire, monkeypatch) as (
        app,
        broker,
        actor,
        project,
        run,
        request,
    ):
        broker.waiting = True
        await app.service.reply_approval(run, request["request_id"], approval(request))
        async with asyncio.timeout(5):
            await broker.entered.wait()
        with pytest.raises(ServiceError, match="workspace_busy"):
            await app.sandbox.provision(
                actor, project_id=project["project_id"], request_key="reprepare"
            )
        await app.service.cancel(run)
        await finished(app, run)
        assert broker.cancelled


async def test_current_project_can_be_read_during_execution_without_claiming_snapshot(
    tmp_path, wire, monkeypatch
):
    async with command_app(tmp_path, wire, monkeypatch) as (
        app,
        broker,
        actor,
        project,
        run,
        request,
    ):
        original = broker.call
        broker.waiting = True

        async def with_files(action, **arguments):
            if action == "read":
                return {
                    "path": arguments["path"],
                    "data": base64.b64encode(b"in progress").decode(),
                }
            return await original(action, **arguments)

        broker.call = with_files
        await app.service.reply_approval(run, request["request_id"], approval(request))
        async with asyncio.timeout(5):
            await broker.entered.wait()
        current = await app.sandbox.read_file(actor, project["project_id"], path="working.txt")
        assert current["may_change"] is True
        assert base64.b64decode(current["data"]) == b"in progress"
        await app.service.cancel(run)
        await finished(app, run)


async def test_bound_session_supplies_missing_command_project_id(tmp_path, wire, monkeypatch):
    async with command_app(tmp_path, wire, monkeypatch, bind_session=True, omit_project=True) as (
        app,
        broker,
        actor,
        project,
        run,
        request,
    ):
        assert "project_id" not in request["arguments"]
        await app.service.reply_approval(run, request["request_id"], approval(request))
        assert (await finished(app, run)).status == "completed"
        assert broker.starts[0]["command"]["project_id"] == project["project_id"]
        source = (await app.store.snapshot(run)).session_context
        from krim_agent.sandbox.bindings import read_binding

        selected = await read_binding(app.sandbox, actor, source.session_id)
        assert selected["project_id"] == project["project_id"]

        selected = (
            await read_binding(app.sandbox, actor, request["session_id"])
            if "session_id" in request
            else None
        )
        assert selected is None or selected["project_id"] == project["project_id"]


@pytest.mark.parametrize(
    "name,body",
    [
        (
            "sandbox_write",
            {
                "expected_generation": 0,
                "path": "note.txt",
                "expected_sha256": None,
                "content": "批准后写入",
            },
        ),
        ("sandbox_mkdir", {"expected_generation": 0, "path": "src/code"}),
        (
            "sandbox_remove",
            {"expected_generation": 0, "path": "note.txt", "expected_sha256": "0" * 64},
        ),
    ],
)
async def test_file_write_tools_wait_for_approval_and_route_to_file_job(
    tmp_path, wire, monkeypatch, name, body
):
    async with command_app(
        tmp_path,
        wire,
        monkeypatch,
        tool_name=name,
        tool_body=body,
        bind_session=True,
        omit_project=True,
    ) as (app, broker, _, project, run, request):
        assert broker.starts == []
        await app.service.reply_approval(run, request["request_id"], approval(request))
        assert (await finished(app, run)).status == "completed"
        assert len(broker.starts) == 1 and "mutation" in broker.starts[0]
        assert broker.starts[0]["mutation"]["project_id"] == project["project_id"]


async def test_bound_project_discovery_is_readonly_and_not_approval_gated(
    tmp_path, wire, monkeypatch
):
    async with command_app(
        tmp_path,
        wire,
        monkeypatch,
        tool_name="sandbox_project",
        tool_body={},
        bind_session=True,
        omit_project=True,
        park=False,
    ) as (app, broker, _, project, run, request):
        assert request is None and broker.starts == []
        ends = [e.data for e in await app.service.events(run) if e.kind == "tool_end"]
        assert ends[0]["decision"] == "allow"
        assert ends[0]["value"]["project_id"] == project["project_id"]


async def test_parked_session_cannot_be_rebound_while_awaiting_approval(
    tmp_path, wire, monkeypatch
):
    from krim_agent.sandbox.bindings import bind_project, read_binding

    async with command_app(tmp_path, wire, monkeypatch, bind_session=True) as (
        app,
        broker,
        actor,
        project,
        run,
        request,
    ):
        session = (await app.store.snapshot(run)).session_context.session_id
        selected = await read_binding(app.sandbox, actor, session)
        with pytest.raises(ServiceError, match="workspace_busy"):
            await bind_project(
                app.sandbox,
                actor,
                session,
                {
                    "project_id": None,
                    "expected_revision": selected["revision"],
                    "expected_session_revision": selected["session_revision"],
                },
                request_key="rebind",
            )
        assert broker.starts == []
        await app.service.reply_approval(run, request["request_id"], approval(request, False))
        await finished(app, run)


async def test_approved_tool_rechecks_binding_even_if_storage_was_changed_outside_api(
    tmp_path, wire, monkeypatch
):
    from krim_agent.storage.sandbox_models import sandbox_session_bindings

    async with command_app(tmp_path, wire, monkeypatch, bind_session=True) as (
        app,
        broker,
        actor,
        project,
        run,
        request,
    ):
        session = (await app.store.snapshot(run)).session_context.session_id
        other = await app.sandbox.create_project(
            actor,
            {"title": "另一个项目", "workspace_id": actor.memberships[0].workspace_id},
            request_key="other-project",
        )
        await app.store._write(
            lambda conn: conn.execute(
                update(sandbox_session_bindings)
                .where(sandbox_session_bindings.c.session_id == session)
                .values(project_id=other["project_id"], revision=2)
            )
        )
        await app.service.reply_approval(run, request["request_id"], approval(request))
        view = await finished(app, run)
        assert view.status == "failed" and broker.starts == []
