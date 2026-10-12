"""真实树Registry发布、原回执前暂停与冷重开；源码和项目外wheel共用。"""

import asyncio
import json
import os
import sys
from pathlib import Path
from threading import Event

from sqlalchemy import func, select

import krim_agent
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.config import Settings
from krim_agent.execution import CommandSpec, LinuxSandboxRunner
from krim_agent.execution.directory_publication import CommandDirectoryDependencyRequest
from krim_agent.persistence.command_directory_dependencies import CommandDirectoryDependencyStore
from krim_agent.persistence.database import (
    WorkspaceDirectoryAttemptRow,
    WorkspaceDirectoryProposalRow,
    WorkspaceEditAttemptRow,
    WorkspaceEditProposalRow,
)
from krim_agent.persistence.workspace_tree_plans import WorkspaceTreePlanStore
from krim_agent.protocol.domain import ServiceError
from krim_agent.providers import FakeProvider
from krim_agent.sdk import AgentCommandRequest, AgentSession, PythonAI
from krim_agent.workspace_directory import ArtifactDirectorySpec, WorkspaceDirectoryWriter
from krim_agent.workspace_tree import TreePlanRequest
from krim_agent.workspace_writer import WorkspaceWriter


class Provider(FakeProvider):
    def __init__(self):
        super().__init__(delay=0)
        self.calls = 0

    async def stream(self, messages, tools):
        self.calls += 1
        async for item in super().stream(messages, tools):
            yield item


async def allow(_definition, _context):
    return "allow"


async def invoke(service, agent, dispatch):
    return await service._workspace_call(
        agent.agent_id,
        dispatch.name,
        dispatch.request.model_dump(),
        confirmed=True,
        tree_fence=dispatch.fence,
    )


async def publish(service, agent, store, execution, position):
    scope, identifier = execution.view.scope_id, execution.view.plan_id
    await invoke(service, agent, await store.dispatch(scope, identifier, execution.token, position))
    dispatch = await store.dispatch(scope, identifier, execution.token, position)
    value = await invoke(service, agent, dispatch)
    assert value["status"] == "done"
    await store.finish_step(scope, identifier, execution.token, position)


async def metadata(service, agent, store, identifier, root, source, provider, *, worker=False):
    scope = "session:" + agent.session_id
    value = await store.get(scope, identifier)
    page = await store.steps(scope, identifier)
    result = {
        "package": krim_agent.__file__,
        "pid": os.getpid(),
        "agent_id": agent.agent_id,
        "scope": scope,
        "plan_id": identifier,
        "source": source,
        "view": value.model_dump(mode="json"),
        "steps": page.model_dump(mode="json"),
        "provider_calls": provider.calls,
        "worker": worker,
    }
    (root / "metadata.json").write_text(json.dumps(result))
    return result


async def produce(ai, root, phase, source, provider, *, worker=False):
    service = ai.agents()
    await service.profile_put(
        AgentProfileUpdate(
            expected_revision=0,
            config=AgentProfile(
                name="tree-process",
                description="目录树围栏独立进程",
                workspace_key="files",
                execution_key="sandbox",
                tool_names=[
                    "command_execute",
                    "command_get",
                    "command_propose",
                    "directory_source_preview",
                    "workspace_change",
                    "directory_change",
                    "workspace_reconcile",
                    "directory_reconcile",
                ],
            ),
        )
    )
    agent = AgentSession(service, (await service.spawn("tree-process", persistent=True)).agent_id)
    command = await agent.execute_command(
        AgentCommandRequest(
            request_key="command",
            command=CommandSpec(
                argv=[
                    "/usr/bin/python3",
                    "-I",
                    "-c",
                    "import os;os.makedirs('tree/sub');os.chmod('tree',0o750);"
                    "os.chmod('tree/sub',0o700);open('tree/sub/data.bin','wb').write(bytes([0,255]))",
                ]
            ),
        ),
        confirmed=True,
    )
    assert command.outcome == "completed"
    scope = "session:" + agent.session_id
    dependency = await CommandDirectoryDependencyStore(service.workspace_edits).plan(
        scope, CommandDirectoryDependencyRequest(command_id=command.command_id)
    )
    binding = service._handle(agent.agent_id).definition.workspace
    prepared = WorkspaceDirectoryWriter(binding.require()).prepare_artifact(
        ArtifactDirectorySpec(
            path="tree",
            operation="create",
            mode=0o750,
            command_id=command.command_id,
            source_hash=dependency.source_hash,
        )
    )
    store = WorkspaceTreePlanStore(service.workspace_edits, service.manager.directory_changes.store)
    view, _ = await store.reserve(
        scope,
        binding,
        TreePlanRequest(
            command_id=command.command_id,
            source_hash=dependency.source_hash,
            root_path="tree",
            request_key="tree",
        ),
        prepared,
    )
    execution = None if worker else await store.claim(scope, view.plan_id)
    if phase == "complete":
        if worker:
            original_execute = service.workspace_trees.execute

            async def checked_close(identifier, dispatch, confirmed):
                try:
                    await ai.close()
                except ServiceError as error:
                    assert error.code == "workspace_tree_close_reentrant" and error.status == 409
                    assert not ai._closed and not service._closed
                else:
                    raise AssertionError("树回调不能关闭SDK或数据库")
                return await original_execute(identifier, dispatch, confirmed)

            service.workspace_trees.execute = checked_close
            await service.workspace_start_tree(agent.agent_id, view.plan_id, confirmed=True)
            done = await service.workspace_wait_tree(
                agent.agent_id, view.plan_id, timeout_seconds=20
            )
            assert done.status == "done"
            assert (
                await service.workspace_start_tree(agent.agent_id, view.plan_id, confirmed=True)
                == done
            )
        else:
            preview = await store.dispatch(scope, view.plan_id, execution.token, 0)
            try:
                await service._workspace_call(
                    agent.agent_id, preview.name, preview.request.model_dump(), confirmed=True
                )
            except ServiceError as error:
                assert error.code == "workspace_tree_request_reserved" and error.status == 403
            else:
                raise AssertionError("公开内部树key不能获得准入")
            for position in range(3):
                await publish(service, agent, store, execution, position)
            assert (await store.finish(scope, view.plan_id, execution.token)).status == "done"
        print(
            json.dumps(
                await metadata(
                    service, agent, store, view.plan_id, root, source, provider, worker=worker
                )
            ),
            flush=True,
        )
        return
    assert phase == "after-write"
    position = 0 if source == "directory" else 2
    if not worker:
        for parent in range(position):
            await publish(service, agent, store, execution, parent)
        await invoke(
            service, agent, await store.dispatch(scope, view.plan_id, execution.token, position)
        )
        dispatch = await store.dispatch(scope, view.plan_id, execution.token, position)
    backend = store.files if source == "file" else store.directories
    original_settle = backend.settle
    entered, release = Event(), Event()

    async def before_receipt(*args, **kwargs):
        entered.set()
        assert await asyncio.to_thread(release.wait, 60)
        return await original_settle(*args, **kwargs)

    backend.settle = before_receipt
    if worker:
        await service.workspace_start_tree(agent.agent_id, view.plan_id, confirmed=True)
        task = service.workspace_trees._tasks[view.plan_id][1]
    else:
        task = asyncio.create_task(invoke(service, agent, dispatch))
    try:
        assert await asyncio.to_thread(entered.wait, 25)
        print(
            json.dumps(
                await metadata(
                    service, agent, store, view.plan_id, root, source, provider, worker=worker
                )
            ),
            flush=True,
        )
        assert (await asyncio.to_thread(sys.stdin.readline)).strip() == "release"
        release.set()
        await task
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def recover(ai, root, phase, provider):
    value = json.loads((root / "metadata.json").read_text())
    replay_calls = 0

    def forbidden(*_args, **_kwargs):
        nonlocal replay_calls
        replay_calls += 1
        raise AssertionError("冷读和核实禁止重发命令/Writer")

    WorkspaceWriter.apply = WorkspaceDirectoryWriter.apply = LinuxSandboxRunner.start = forbidden
    service = ai.agents()
    store = WorkspaceTreePlanStore(service.workspace_edits, service.manager.directory_changes.store)
    if phase == "recover":
        assert await service.restore_persistent() == 1
        agent = AgentSession(service, value["agent_id"])
        position = 0 if value["source"] == "directory" else 2
        operation_id = value["steps"]["items"][position]["operation"]["operation_id"]
        if value["source"] == "directory":
            await store.directories.operation(value["scope"], operation_id)
        current = await store.get(value["scope"], value["plan_id"])
        assert current.producer_status == "interrupted"
        if value.get("worker"):
            assert (
                await service.workspace_start_tree(agent.agent_id, value["plan_id"], confirmed=True)
                == current
            )
            assert not service.workspace_trees._tasks
        page = await store.steps(value["scope"], value["plan_id"])
        assert page.items[position].status == "uncertain"
        resolved = await (
            agent.reconcile_file_change(operation_id)
            if value["source"] == "file"
            else agent.reconcile_directory_change(operation_id)
        )
        assert resolved.status == "done"
        page = await store.steps(value["scope"], value["plan_id"])
        assert page.items[position].operation.operation_id == operation_id
        assert all(
            item.status == "skipped" and item.operation is None
            for item in page.items[position + 1 :]
        )
    else:
        assert phase == "read"
        assert (await store.get(value["scope"], value["plan_id"])).model_dump(mode="json") == value[
            "view"
        ]
        assert (await store.steps(value["scope"], value["plan_id"])).model_dump(
            mode="json"
        ) == value["steps"]
    current = await store.get(value["scope"], value["plan_id"])
    assert await store.by_key(value["scope"], "tree") == current
    async with store.database.sessions() as sql:
        counts = [
            await sql.scalar(select(func.count()).select_from(model))
            for model in (
                WorkspaceDirectoryProposalRow,
                WorkspaceDirectoryAttemptRow,
                WorkspaceEditProposalRow,
                WorkspaceEditAttemptRow,
            )
        ]
    assert counts == (
        [1, 1, 0, 0] if phase == "recover" and value["source"] == "directory" else [2, 2, 1, 1]
    )
    assert (root / "files/tree").exists()
    if phase == "read" or value["source"] == "file":
        assert (root / "files/tree/sub/data.bin").read_bytes() == b"\x00\xff"
    else:
        assert not (root / "files/tree/sub").exists()
    assert replay_calls == provider.calls == 0
    print(
        json.dumps(
            {
                "package": krim_agent.__file__,
                "counts": counts,
                "view": current.model_dump(mode="json"),
                "replay_calls": replay_calls,
                "provider_calls": provider.calls,
            }
        ),
        flush=True,
    )


async def main():
    phase, source, folder = sys.argv[1:4]
    root, provider = Path(folder), Provider()
    async with await PythonAI.create(
        settings=Settings(database_url=f"sqlite+aiosqlite:///{root / 'state.sqlite'}"),
        provider=provider,
    ) as ai:
        ai.runtime.tool_policy = allow
        ai.agents().register_workspace("files", root / "files", version="v1", writable=True)
        ai.agents().register_execution("sandbox", LinuxSandboxRunner(), version="v1", owned=True)
        if phase in {"read", "recover"}:
            await recover(ai, root, phase, provider)
        else:
            await produce(
                ai,
                root,
                phase.removeprefix("worker-"),
                source,
                provider,
                worker=phase.startswith("worker-"),
            )


asyncio.run(main())
