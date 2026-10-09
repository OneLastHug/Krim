"""真实Agent命令：权限、完整输入、Hook、资源与原任务消费。"""

import asyncio
import base64
import json
import shutil
import sys
from dataclasses import replace

import pytest
from sqlalchemy import select

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.application.agents import AgentService
from krim_agent.execution import CommandSpec, LinuxSandboxRunner
from krim_agent.execution.agent_models import AgentCommandRequest, CommandLookup, CommandRead
from krim_agent.persistence.database import ToolAuditRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.sdk.agents import AgentSession
from krim_agent.tools.core import ShortCircuit, ToolContext

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux") or shutil.which("bwrap") is None,
    reason="需要真实Linux隔离执行",
)

NAMES = ["workspace_read", "command_execute", "command_get", "command_read", "command_cancel"]


class CountingRunner(LinuxSandboxRunner):
    def __init__(self):
        super().__init__()
        self.calls = 0

    async def start(self, *args, **kwargs):
        self.calls += 1
        return await super().start(*args, **kwargs)


async def setup(runtime, root, *, policy=None, names=None, mode="agent", persistent=False):
    async def allow(*_args):
        return "allow"

    runtime.tool_policy = policy or allow
    service = AgentService(runtime, runtime.settings)
    service.register_workspace("files", root, version="1", writable=False)
    runner = CountingRunner()
    service.register_execution("sandbox", runner, version="1", owned=True)
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="command-worker",
                description="命令测试",
                workspace_key="files",
                execution_key="sandbox",
                tool_names=names if names is not None else NAMES,
                mode=mode,
                persistent=persistent,
                tool_timeout_seconds=20,
            ),
        )
    )
    view = await service.spawn("command-worker")
    return service, AgentSession(service, view.agent_id), runner


def request(code="print('ok')", *, key="one", paths=()):
    return AgentCommandRequest(
        request_key=key,
        command=CommandSpec(argv=["/usr/bin/python3", "-I", "-c", code]),
        input_paths=list(paths),
    )


async def command_ready(service, session, key="one"):
    scope = "session:" + session.session_id

    async def wait():
        while True:
            view = await service.manager.commands.store.by_key(scope, key)
            if view and view.started and view.stdout_bytes:
                return view
            await asyncio.sleep(0.02)

    return await asyncio.wait_for(wait(), 10)


async def test_complete_readonly_inputs_original_key_and_binary_artifact(runtime, tmp_path):
    text = "full input\n" * 6000 + "TAIL"
    file = tmp_path / "large.txt"
    file.write_text(text)
    service, session, runner = await setup(runtime, tmp_path)
    command = request(
        "from pathlib import Path;data=Path('large.txt').read_text();print(len(data),data[-4:]);"
        "Path('out.bin').write_bytes(b'\\0\\xff')",
        paths=["large.txt"],
    )
    try:
        result = await session.execute_command(command)
        assert result.outcome == "completed" and result.processes_reaped
        assert result.stdout_text == f"{len(text)} TAIL\n"
        assert file.read_text() == text and not (tmp_path / "out.bin").exists()
        content = await session.read_command(
            CommandRead(command_id=result.command_id, source="artifact", path="out.bin")
        )
        assert base64.b64decode(content.data_base64) == b"\0\xff"
        scope = "session:" + session.session_id
        saved = await service.manager.commands.store.files(scope, result.command_id, "input")
        assert saved["large.txt"].data == text.encode()
        origin = await service.manager.commands.store.origin(scope, result.command_id)
        assert json.loads(origin["captures"][0])["snapshot"]["mtime_ns"] > 9007199254740991
        file.write_text("after command")
        assert (await session.execute_command(command)).command_id == result.command_id
        assert runner.calls == 1 and file.read_text() == "after command"
        with pytest.raises(ServiceError) as conflict:
            await session.execute_command(request("print('different')", paths=["large.txt"]))
        assert conflict.value.code == "execution_key_conflict"
        await session.close()
        assert (
            await session.command(CommandLookup(request_key="one"))
        ).command_id == result.command_id
        with pytest.raises(ServiceError) as closed:
            await session.execute_command(request(key="new"))
        assert closed.value.code == "agent_session_closed"
    finally:
        await service.close()
    assert runner.closed


@pytest.mark.parametrize(
    "decision,confirmed,expected",
    [("deny", True, "permission_denied"), ("ask", False, "approval_required"), ("ask", True, None)],
)
async def test_sdk_confirmation_covers_only_frozen_command_and_named_inputs(
    runtime, tmp_path, decision, confirmed, expected
):
    (tmp_path / "in.txt").write_text("input")

    async def policy(*_args):
        return decision

    service, session, runner = await setup(runtime, tmp_path, policy=policy)
    try:
        if expected:
            with pytest.raises(ServiceError) as denied:
                await session.execute_command(request(paths=["in.txt"]), confirmed=confirmed)
            assert denied.value.code == expected and runner.calls == 0
        else:
            result = await session.execute_command(request(paths=["in.txt"]), confirmed=confirmed)
            assert result.outcome == "completed" and runner.calls == 1
            audit = service._handle(session.agent_id).runtime.tools.audit
            assert {item.name for item in audit if item.decision == "allow"} >= {
                "command_execute",
                "workspace_read",
            }
    finally:
        await service.close()


@pytest.mark.parametrize("mode", ["ask", "plan", "auto"])
async def test_modes_do_not_expand_command_permission(runtime, tmp_path, mode):
    service, session, runner = await setup(runtime, tmp_path, mode=mode)
    try:
        if mode in {"ask", "plan"}:
            with pytest.raises(ServiceError) as denied:
                await session.execute_command(request(), confirmed=True)
            assert denied.value.code == "permission_denied" and runner.calls == 0
        else:
            assert (await session.execute_command(request())).outcome == "completed"
            assert service._handle(session.agent_id).runtime.tools.audit[-1].decision == "audit"
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["short", "path", "range", "post", "deny", "missing_tool"])
async def test_input_chain_rejects_forged_changed_or_unpermitted_capture(runtime, tmp_path, fault):
    (tmp_path / "in.txt").write_text("original")
    (tmp_path / "other.txt").write_text("other")

    async def policy(definition, _context):
        return "deny" if fault == "deny" and definition.name == "workspace_read" else "allow"

    service, session, runner = await setup(
        runtime, tmp_path, policy=policy, names=NAMES[1:] if fault == "missing_tool" else None
    )
    registry = service._handle(session.agent_id).runtime.tools

    async def pre(definition, context):
        if definition.name != "workspace_read":
            return None
        if fault == "short":
            return ShortCircuit(
                service._handle(session.agent_id)
                .definition.workspace.workspace.read("in.txt")
                .model_dump(mode="json")
            )
        if fault == "path":
            context.arguments["path"] = "other.txt"
        if fault == "range":
            context.arguments["start_line"] = 2

    async def post(definition, value, _context):
        if fault == "post" and definition.name == "workspace_read":
            value["content"] = "redacted"
        return value

    registry.add_global_pre(pre)
    registry.add_global_post(post)
    try:
        with pytest.raises(ServiceError) as rejected:
            await session.execute_command(request(paths=["in.txt"]))
        assert rejected.value.code in {
            "execution_input_not_captured",
            "execution_input_target",
            "permission_denied",
            "tool_not_found",
        }
        assert runner.calls == 0
        assert (
            await service.manager.commands.store.by_key("session:" + session.session_id, "one")
            is None
        )
    finally:
        await service.close()


async def test_input_changed_between_reads_is_not_dispatched(runtime, tmp_path):
    first = tmp_path / "a.txt"
    first.write_text("before")
    (tmp_path / "b.txt").write_text("second")
    service, session, runner = await setup(runtime, tmp_path)

    async def mutate(definition, value, context):
        if definition.name == "workspace_read" and context.arguments["path"] == "b.txt":
            await asyncio.to_thread(first.write_text, "after")
        return value

    service._handle(session.agent_id).runtime.tools.add_global_post(mutate)
    try:
        with pytest.raises(ServiceError) as changed:
            await session.execute_command(request(paths=["a.txt", "b.txt"]))
        assert changed.value.code == "workspace_checkpoint_changed" and runner.calls == 0
    finally:
        await service.close()


@pytest.mark.parametrize("stop", ["caller", "resource", "workspace", "close"])
async def test_stopping_agent_command_reaps_original_and_keeps_receipt(runtime, tmp_path, stop):
    service, session, runner = await setup(runtime, tmp_path)
    task = asyncio.create_task(
        session.execute_command(request("import time;print('ready',flush=True);time.sleep(30)"))
    )
    try:
        running = await command_ready(service, session)
        if stop == "caller":
            task.cancel()
        elif stop == "resource":
            service.disable_execution("sandbox")
        elif stop == "workspace":
            service.disable_workspace("files")
        else:
            await service.close()
        outcome = (await asyncio.gather(task, return_exceptions=True))[0]
        assert isinstance(outcome, (asyncio.CancelledError, ServiceError)), outcome
        stored = await service.manager.commands.store.get(
            "session:" + session.session_id, running.command_id
        )
        assert stored.phase == "finished" and stored.processes_reaped
        assert stored.outcome == "cancelled" and not runner._active
        if stop == "resource":
            assert (
                await session.command(CommandLookup(command_id=running.command_id))
            ).processes_reaped
    finally:
        await service.close()


async def test_callback_cannot_close_its_own_service(runtime, tmp_path):
    (tmp_path / "in.txt").write_text("input")
    service, session, _ = await setup(runtime, tmp_path)

    async def pre(definition, _context):
        if definition.name == "workspace_read":
            with pytest.raises(ServiceError) as reentry:
                await service.close()
            assert reentry.value.code == "execution_command_reentrant"

    service._handle(session.agent_id).runtime.tools.add_global_pre(pre)
    try:
        assert (await session.execute_command(request(paths=["in.txt"]))).outcome == "completed"
        assert not service._closed
    finally:
        await service.close()


async def test_model_command_loop_uses_original_run_call_and_persistent_audit(runtime, tmp_path):
    (tmp_path / "in.txt").write_text("model input")
    command = request("print(open('in.txt').read())", key="model", paths=["in.txt"])

    class Provider:
        name = "command-loop"
        calls = 0

        async def stream(self, messages, tools):
            self.calls += 1
            assert "command_execute" in {item.name for item in tools}
            if self.calls == 1:
                yield ModelToolCall(
                    id="original-command-call",
                    name="command_execute",
                    arguments=command.model_dump(mode="json"),
                )
            else:
                value = json.loads(messages[-1].content)
                assert value["outcome"] == "completed" and value["stdout_text"] == "model input\n"
                yield ModelText(text="command finished")

    runtime.provider = Provider()
    service, session, runner = await setup(runtime, tmp_path)
    try:
        result = await session.execute("run command", idempotency_key="turn")
        assert result.status == "completed" and result.text == "command finished", result
        saved = await service.manager.commands.store.by_key(
            "session:" + session.session_id, "model"
        )
        assert saved.run_id == result.run_id and saved.call_id == "original-command-call"
        async with runtime.repository.database.sessions() as db:
            rows = list(
                await db.scalars(select(ToolAuditRow).where(ToolAuditRow.run_id == result.run_id))
            )
        assert {row.payload["name"] for row in rows} >= {"command_execute", "workspace_read"}
        assert runner.calls == 1
    finally:
        await service.close()


async def test_parent_resource_and_command_tool_scope_cannot_be_expanded(runtime, tmp_path):
    service, session, _ = await setup(runtime, tmp_path)
    parent = service._handle(session.agent_id)
    other = CountingRunner()
    binding = service.register_execution("other", other, version="1", owned=True)
    from krim_agent.agents import AgentError

    try:
        with pytest.raises(AgentError) as different:
            await service.manager.spawn(
                replace(parent.definition, agent_id="", execution=binding),
                parent_context=ToolContext(
                    parent.runtime.tools,
                    agent_id=parent.agent_id,
                    session_id=parent.session_id,
                    execution=parent.runtime.execution,
                    workspace=parent.runtime.workspace,
                ),
            )
        assert different.value.code == "agent_execution_forbidden"
        with pytest.raises(AgentError) as more_tools:
            await service.manager.spawn(
                replace(parent.definition, agent_id=""),
                parent_context=ToolContext(
                    parent.runtime.tools.select(["workspace_read", "command_execute"]),
                    agent_id=parent.agent_id,
                    session_id=parent.session_id,
                    execution=parent.runtime.execution,
                    workspace=parent.runtime.workspace,
                ),
            )
        assert more_tools.value.code == "agent_execution_forbidden"
    finally:
        await service.close()


async def test_cancel_tool_cannot_cross_sessions_and_waits_for_original_process(runtime, tmp_path):
    service, first, runner = await setup(runtime, tmp_path)
    second = AgentSession(service, (await service.spawn("command-worker")).agent_id)
    task = asyncio.create_task(
        first.execute_command(request("import time;print('ready',flush=True);time.sleep(30)"))
    )
    try:
        running = await command_ready(service, first)
        lookup = CommandLookup(command_id=running.command_id)
        for operation in (second.command, second.cancel_command):
            with pytest.raises(ServiceError) as hidden:
                await operation(lookup)
            assert hidden.value.code == "execution_not_found"
        assert runner._active
        result = await first.cancel_command(lookup)
        assert result.processes_reaped and result.outcome == "cancelled"
        assert (await task).outcome == "cancelled" and not runner._active
    finally:
        await service.close()


async def test_tool_deadline_cancels_actual_command_before_returning_timeout(runtime, tmp_path):
    service, session, runner = await setup(runtime, tmp_path)
    deadlines = []

    async def pre(definition, context):
        if definition.name == "command_execute":
            deadlines.append(context.execution_timeouts[-1].deadline)

    service._handle(session.agent_id).runtime.tools.add_global_pre(pre)
    task = asyncio.create_task(
        session.execute_command(request("import time;print('ready',flush=True);time.sleep(30)"))
    )
    try:
        running = await command_ready(service, session)
        deadlines[0].reschedule(asyncio.get_running_loop().time())
        with pytest.raises(ServiceError) as timed:
            await task
        assert timed.value.code == "timeout"
        saved = await service.manager.commands.store.get(
            "session:" + session.session_id, running.command_id
        )
        assert saved.outcome == "cancelled" and saved.processes_reaped and not runner._active
    finally:
        await service.close()


async def test_execution_resource_is_explicit_and_version_is_frozen_on_restore(runtime, tmp_path):
    service, session, runner = await setup(runtime, tmp_path, persistent=True)
    try:
        default = await service.definition("default")
        assert not any(item.name.startswith("command_") for item in default.tools.definitions)
        resource = next(item for item in service.resource_inventory() if item.kind == "execution")
        assert resource.key == "sandbox" and resource.version == "1" and resource.owned
        assert "/usr/bin" not in resource.model_dump_json()
        await session.execute_command(request())
        identifier = session.agent_id
    finally:
        await service.close()
    assert runner.closed

    async def cold(version):
        created = AgentService(runtime, runtime.settings)
        created.register_workspace("files", tmp_path, version="1", writable=False)
        created.register_execution("sandbox", CountingRunner(), version=version, owned=True)
        return created

    wrong = await cold("2")
    try:
        assert await wrong.restore_persistent() == 0
        with pytest.raises(ServiceError) as changed:
            await wrong.restore(identifier)
        assert changed.value.code == "agent_definition_changed"
    finally:
        await wrong.close()
    correct = await cold("1")
    try:
        assert await correct.restore_persistent() == 1
        restored = AgentSession(correct, identifier)
        assert (await restored.command(CommandLookup(request_key="one"))).outcome == "completed"
        assert correct.resources.execution("sandbox").runner.calls == 0
    finally:
        await correct.close()


@pytest.mark.parametrize("fault", ["symlink", "hardlink", "binary", "mode"])
async def test_input_file_types_cannot_bypass_original_workspace_rules(runtime, tmp_path, fault):
    import os

    file = tmp_path / "in.txt"
    if fault == "symlink":
        file.symlink_to("/etc/passwd")
    elif fault == "hardlink":
        (tmp_path / "other").write_text("data")
        os.link(tmp_path / "other", file)
    else:
        file.write_bytes(b"\0data" if fault == "binary" else b"data")
        if fault == "mode":
            file.chmod(0o2644)
    service, session, runner = await setup(runtime, tmp_path)
    try:
        with pytest.raises(ServiceError) as rejected:
            await session.execute_command(request(paths=["in.txt"]))
        assert rejected.value.code in {
            "workspace_access",
            "workspace_link",
            "workspace_binary",
            "execution_input_file",
        }
        assert runner.calls == 0
    finally:
        await service.close()


async def test_large_output_and_artifact_listing_stay_within_tool_contract(runtime, tmp_path):
    service, session, _ = await setup(runtime, tmp_path)
    try:
        result = await session.execute_command(
            request(
                "import os;os.write(1,b'\\0'*60000);"
                "[open(f'{i}.txt','w').write('x') for i in range(40)]"
            )
        )
        assert (
            result.outcome == "completed"
            and result.stdout_bytes == 60000
            and result.output_truncated
        )
        assert (
            len(result.artifacts) == 32
            and result.artifacts_truncated
            and result.artifact_files == 40
        )
        part = await session.read_command(
            CommandRead(command_id=result.command_id, offset=59000, limit=4096)
        )
        assert base64.b64decode(part.data_base64) == b"\0" * 1000
        assert part.next_offset == 60000 and not part.truncated
    finally:
        await service.close()


@pytest.mark.parametrize("fault", ["short", "post"])
async def test_outer_hook_cannot_forge_an_actual_command_receipt(runtime, tmp_path, fault):
    service, session, runner = await setup(runtime, tmp_path)
    try:
        original = await session.execute_command(request())
        registry = service._handle(session.agent_id).runtime.tools

        async def pre(definition, _context):
            if fault == "short" and definition.name == "command_execute":
                return ShortCircuit(original.model_dump(mode="json"))

        async def post(definition, value, _context):
            if fault == "post" and definition.name == "command_execute":
                value["exit_code"] = 0
                value["outcome"] = "completed"
            return value

        registry.add_global_pre(pre)
        registry.add_global_post(post)
        with pytest.raises(ServiceError) as forged:
            await session.execute_command(request("raise SystemExit(7)", key="second"))
        assert forged.value.code == "execution_receipt_unverified"
        saved = await service.manager.commands.store.by_key(
            "session:" + session.session_id, "second"
        )
        if fault == "short":
            assert saved is None and runner.calls == 1
        else:
            assert saved.exit_code == 7 and saved.outcome == "failed" and runner.calls == 2
    finally:
        await service.close()


async def test_child_inherits_usable_execution_and_parent_close_stops_it(runtime, tmp_path):
    service, parent_session, runner = await setup(runtime, tmp_path)
    parent = service._handle(parent_session.agent_id)
    try:
        child = await service.manager.spawn(
            replace(parent.definition, agent_id=""),
            parent_context=ToolContext(
                parent.runtime.tools,
                agent_id=parent.agent_id,
                session_id=parent.session_id,
                execution=parent.runtime.execution,
                workspace=parent.runtime.workspace,
                policy=parent.runtime.tool_policy,
            ),
        )
        session = AgentSession(service, child.agent_id)
        assert (await session.execute_command(request(key="allowed"))).outcome == "completed"
        task = asyncio.create_task(
            session.execute_command(request("import time;print('ready',flush=True);time.sleep(30)"))
        )
        running = await command_ready(service, session)
        await parent_session.close()
        with pytest.raises(ServiceError) as closed:
            await task
        assert closed.value.code == "agent_parent_session_closed"
        saved = await service.manager.commands.store.get(
            "session:" + session.session_id, running.command_id
        )
        assert saved.processes_reaped and not runner._active
    finally:
        await service.close()


@pytest.mark.parametrize("action", ["approve", "deny"])
async def test_model_permission_request_controls_actual_command_start(runtime, tmp_path, action):
    from krim_agent.interactions import InteractionReply

    class Provider:
        name = "command-approval"
        calls = 0

        async def stream(self, messages, tools):
            self.calls += 1
            if self.calls == 1:
                yield ModelToolCall(
                    id="approval-call",
                    name="command_execute",
                    arguments=request().model_dump(mode="json"),
                )
            else:
                yield ModelText(text="finished")

    async def policy(definition, _context):
        return "ask" if definition.risk == "write" else "allow"

    runtime.provider = Provider()
    service, session, runner = await setup(runtime, tmp_path, policy=policy)
    task = asyncio.create_task(session.execute("request command", idempotency_key="turn"))

    async def pending():
        while True:
            current = await session.state()
            if current.current_run_id:
                rows = await runtime.repository.list_interactions(current.current_run_id)
                if rows:
                    return current.current_run_id, rows[0]
            await asyncio.sleep(0.02)

    try:
        run_id, interaction = await asyncio.wait_for(pending(), 10)
        assert interaction.kind == "approval" and interaction.call_id == "approval-call"
        assert (
            runner.calls == 0
            and await service.manager.commands.store.by_key("session:" + session.session_id, "one")
            is None
        )
        child_runtime = service._handle(session.agent_id).runtime
        await child_runtime.interactions.reply(
            run_id, interaction.id, InteractionReply(action=action)
        )
        await task
        saved = await service.manager.commands.store.by_key("session:" + session.session_id, "one")
        if action == "approve":
            assert saved.outcome == "completed" and runner.calls == 1
        else:
            assert saved is None and runner.calls == 0
        assert (await runtime.repository.list_interactions(run_id))[0].status == (
            "approved" if action == "approve" else "denied"
        )
    finally:
        await service.close()


@pytest.mark.parametrize("path", ["../outside", "/etc/passwd", ".env", ".", "._krim-tx-private"])
def test_command_input_request_rejects_forbidden_workspace_paths(path):
    with pytest.raises(ValueError):
        request(paths=[path])


async def test_model_input_approval_explicitly_covers_complete_file(runtime, tmp_path):
    from krim_agent.interactions import InteractionReply

    (tmp_path / "in.txt").write_text("full file\n" * 500)

    class Provider:
        name = "full-input-approval"
        calls = 0

        async def stream(self, messages, tools):
            self.calls += 1
            if self.calls == 1:
                yield ModelToolCall(
                    id="full-input-parent",
                    name="command_execute",
                    arguments=request(
                        "print(len(open('in.txt').readlines()))", paths=["in.txt"]
                    ).model_dump(mode="json"),
                )
            else:
                yield ModelText(text="finished")

    async def policy(*_args):
        return "ask"

    runtime.provider = Provider()
    service, session, runner = await setup(runtime, tmp_path, policy=policy)
    task = asyncio.create_task(session.execute("use full input", idempotency_key="turn"))

    async def pending(count):
        while True:
            state = await session.state()
            if state.current_run_id:
                items = await runtime.repository.list_interactions(state.current_run_id)
                if len(items) >= count:
                    return state.current_run_id, items[-1]
            await asyncio.sleep(0.02)

    try:
        run_id, main = await asyncio.wait_for(pending(1), 10)
        reader = service._handle(session.agent_id).runtime
        await reader.interactions.reply(run_id, main.id, InteractionReply(action="approve"))
        _, file = await asyncio.wait_for(pending(2), 10)
        assert runner.calls == 0
        assert file.tool_name == "workspace_read" and file.payload["input_capture"] == {
            "scope": "complete_file",
            "purpose": "command_input",
            "path": "in.txt",
            "parent_call_id": "full-input-parent",
        }
        await reader.interactions.reply(run_id, file.id, InteractionReply(action="approve"))
        assert (await task).status == "completed" and runner.calls == 1
        saved = await service.manager.commands.store.by_key("session:" + session.session_id, "one")
        assert await service.manager.commands.store.output(saved.scope_id, saved.command_id) == (
            b"500\n",
            b"",
        )
    finally:
        await service.close()
