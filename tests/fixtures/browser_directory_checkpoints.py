"""显式浏览器测试夹具：真实目录/原模型/受控暂停，生产API无新增旁路。"""

import asyncio
import json
from contextvars import ContextVar
from typing import Literal

from pydantic import Field
from sqlalchemy import select

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.persistence.database import (
    WorkspaceDirectoryCaptureModelSourceRow,
    WorkspaceDirectoryCaptureRow,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader


def configure_directory_checkpoints(app, root):
    service = app.state.agents
    service.register_directory_checkpoints()
    service.register_directory_checkpoint_controls()
    actor_context: ContextVar[str | None] = ContextVar("browser_directory_actor", default=None)
    reads: dict[str, int] = {}
    gates: dict[str, tuple[asyncio.Event, asyncio.Event]] = {}
    failures: set[str] = set()
    providers = {}
    capture = WorkspaceTreeSnapshotReader.capture
    metadata = service.directory_checkpoints.capture_metadata
    record = service.directory_checkpoint_store.record

    def observe(reader, path, **kwargs):
        actor = actor_context.get()
        if actor is not None:
            reads[actor] = reads.get(actor, 0) + 1
        return capture(reader, path, **kwargs)

    async def captured(payload, context):
        token = actor_context.set(context.agent_id)
        try:
            return await metadata(payload, context)
        finally:
            actor_context.reset(token)

    async def save(scope, fence, *args, **kwargs):
        if scope.removeprefix("session:") in failures:
            raise ServiceError("test_directory_material_failure", "合成目录材料故障", 503)
        return await record(scope, fence, *args, **kwargs)

    async def pause(definition, context):
        gate = gates.get(context.agent_id)
        if gate is not None and definition.name == "workspace_directory_checkpoint_capture":
            gate[0].set()
            await gate[1].wait()

    WorkspaceTreeSnapshotReader.capture = observe
    service.directory_checkpoints.capture_metadata = captured
    service.directory_checkpoint_store.record = save
    service.runtime.tools.add_global_pre(pause)

    class Actor(StrictModel):
        name: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,39}$")
        root_path: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,59}$")
        model: bool = False
        leaf: bool = True
        mode: Literal["agent", "plan", "ask"] = "agent"
        writable: bool = True
        isolated_workspace: bool = False
        title: str = Field(default="浏览器目录快照", max_length=120)

    class Control(StrictModel):
        agent_id: str = Field(pattern=r"^[a-f0-9-]{36}$")
        action: Literal["block", "release", "gate", "fail_save", "disable"]

    class Provider:
        name = "offline-browser-directory"

        def __init__(self, payload):
            self.payload, self.calls, self.checkpoint_id = payload, 0, None

        async def stream(self, messages, _tools):
            self.calls += 1
            if self.calls == 1:
                yield ModelToolCall(
                    id="browser-directory-create",
                    name="workspace_directory_checkpoint_create",
                    arguments={
                        "title": self.payload.title,
                        "root_path": self.payload.root_path,
                        "request_key": self.payload.name + "-model",
                    },
                )
            elif self.calls == 2:
                value = json.loads(messages[-1].content)
                assert value["status"] == "available"
                self.checkpoint_id = value["checkpoint_id"]
                yield ModelToolCall(
                    id="browser-directory-query",
                    name="workspace_directory_checkpoint_query",
                    arguments={"kind": "id", "checkpoint_id": self.checkpoint_id},
                )
            else:
                assert (
                    json.loads(messages[-1].content)["checkpoint"]["checkpoint_id"]
                    == self.checkpoint_id
                )
                yield ModelText(text="原目录检查点已创建并核实")

        async def close(self):
            pass

    @app.post("/_test/directory-checkpoint-agent")
    async def actor(payload: Actor):
        tree = root / payload.root_path
        if tree.exists():
            raise ServiceError("test_directory_exists", "测试目录已存在", 409)
        (tree / "sub").mkdir(parents=True)
        (tree / "empty-dir").mkdir()
        (tree / "text.txt").write_bytes(b"directory text\n")
        (tree / "sub/raw.bin").write_bytes(b"\0\xff")
        (tree / "sub/raw.bin").chmod(0o640)
        (tree / "empty.bin").write_bytes(b"")
        names = ["workspace_directory_checkpoint_capture"] if payload.leaf else ["workspace_read"]
        workspace = "edit"
        if not payload.writable or payload.isolated_workspace:
            workspace = "isolated_" + payload.name
            service.register_workspace(
                workspace, root, version="browser-isolated-v1", writable=payload.writable
            )
        provider_key = "runtime"
        if payload.model:
            names += [
                "workspace_directory_checkpoint_create",
                "workspace_directory_checkpoint_query",
                "workspace_directory_checkpoint_cancel",
            ]
            provider_key = "dir_" + payload.name
            providers[payload.name] = provider = Provider(payload)
            service.register_provider(
                provider_key, provider, version="directory-browser-v1", owned=True
            )
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name=payload.name,
                    description="真实目录浏览器",
                    workspace_key=workspace,
                    tool_names=names,
                    mode=payload.mode,
                    provider_key=provider_key,
                ),
            )
        )
        return (await service.spawn(payload.name)).model_dump(mode="json")

    @app.post("/_test/directory-checkpoint-control")
    async def control(payload: Control):
        session = await service.session(payload.agent_id)
        gate = gates.get(payload.agent_id)
        if payload.action == "block":
            if gate is not None:
                raise ServiceError("test_directory_blocked", "测试目录捕获已暂停", 409)
            gates[payload.agent_id] = gate = (asyncio.Event(), asyncio.Event())
        elif payload.action == "release" and gate is not None:
            gate[1].set()
            gates.pop(payload.agent_id, None)
        elif payload.action == "fail_save":
            failures.add(payload.agent_id)
        elif payload.action == "disable":
            service.disable_workspace(session.permissions["workspace"]["key"])
        return {
            "entered": gate is not None and gate[0].is_set(),
            "blocked": gate is not None and not gate[1].is_set(),
        }

    @app.get("/_test/directory-checkpoint-proof/{agent_id}")
    async def proof(agent_id: str):
        scope = "session:" + (await service.session(agent_id)).session_id
        store = service.directory_checkpoint_store
        entries = []
        async with store.database.write_lock, store.database.sessions() as sql:
            rows = await sql.scalars(
                select(WorkspaceDirectoryCaptureRow)
                .where(WorkspaceDirectoryCaptureRow.scope_id == scope)
                .order_by(WorkspaceDirectoryCaptureRow.created_at)
            )
            for row in rows:
                source = await sql.get(WorkspaceDirectoryCaptureModelSourceRow, row.checkpoint_id)
                material = await store._material(sql, row)
                entries.append(
                    {
                        "checkpoint_id": row.checkpoint_id,
                        "status": row.status,
                        "request_key": row.request_key,
                        "source": {"run_id": source.run_id, "call_id": source.call_id}
                        if source is not None
                        else None,
                        "files": {
                            entry.path: {"hex": entry.file.data.hex(), "mode": entry.file.mode}
                            for entry in material.snapshot.entries
                            if entry.file is not None
                        }
                        if material is not None
                        else None,
                    }
                )
        actor = await service.get(agent_id)
        provider = providers.get(actor.name)
        return {
            "reads": reads.get(agent_id, 0),
            "provider_calls": provider.calls if provider else 0,
            "entries": entries,
        }
