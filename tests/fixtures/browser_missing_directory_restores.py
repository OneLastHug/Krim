"""显式本机浏览器目录恢复夹具；实际Model/SQL/Reader/Writer与安全公开事实。"""

import asyncio
import json
import os
from contextvars import ContextVar
from typing import Literal

from pydantic import Field

import krim_agent
import krim_agent.workspace_missing_directory_writer as physical
from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.providers import ModelText, ModelToolCall
from krim_agent.sdk import AgentSession, DirectoryCheckpointCreate
from krim_agent.workspace import WORKSPACE_MISSING_DIRECTORY_RESTORE_CONTROL_TOOL_NAMES
from krim_agent.workspace_directory_absence import WorkspaceDirectoryAbsenceReader
from krim_agent.workspace_directory_restore import DirectoryRestorePreview, DirectoryRestoreTarget
from krim_agent.workspace_directory_restore_control import DIRECTORY_RESTORE_STEP_TOOLS
from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
)
from krim_agent.workspace_missing_directory_writer import WorkspaceMissingDirectoryRestoreWriter


def configure_missing_directory_restores(app, root):
    service = app.state.agents
    service.register_directory_checkpoints()
    service.register_missing_directory_restores()
    service.register_missing_directory_checkpoint_restore_controls()
    actor_context = ContextVar("browser_missing_directory_restore_actor", default=None)
    counts, providers, roots, gates, uncertain = {}, {}, {}, {}, set()
    workspace_groups = set()
    step = service.missing_directory_restores.tool_step
    capture, prepare, apply, reconcile, rename = (
        WorkspaceDirectoryAbsenceReader.capture,
        WorkspaceMissingDirectoryRestoreWriter.prepare,
        WorkspaceMissingDirectoryRestoreWriter.apply,
        WorkspaceMissingDirectoryRestoreWriter.reconcile,
        physical.rename_at,
    )

    def observe(name, operation):
        def call(*args, **kwargs):
            actor = actor_context.get()
            if actor is not None:
                values = counts.setdefault(actor, {})
                values[name] = values.get(name, 0) + 1
            return operation(*args, **kwargs)

        return call

    async def invoke(kind, payload, context):
        token = actor_context.set(context.agent_id)
        try:
            return await step(kind, payload, context)
        finally:
            actor_context.reset(token)

    def lost_reply(*args, **kwargs):
        actor = actor_context.get()
        result = rename(*args, **kwargs)
        if actor in uncertain:
            uncertain.remove(actor)
            raise OSError("synthetic namespace published before reply loss")
        return result

    async def pause(definition, context):
        entry = gates.get(context.agent_id)
        if entry and definition.name == MISSING_DIRECTORY_RESTORE_STEP_TOOLS[entry[0]]:
            entry[1].set()
            await entry[2].wait()

    WorkspaceDirectoryAbsenceReader.capture = observe("capture", capture)
    WorkspaceMissingDirectoryRestoreWriter.prepare = observe("prepare", prepare)
    WorkspaceMissingDirectoryRestoreWriter.apply = observe("apply", apply)
    WorkspaceMissingDirectoryRestoreWriter.reconcile = observe("verify", reconcile)
    physical.rename_at = observe("rename", lost_reply)
    service.missing_directory_restores.tool_step = invoke
    service.runtime.tools.add_global_pre(pause)

    class Actor(StrictModel):
        name: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,39}$")
        mode: Literal["agent", "plan", "ask"] = "agent"
        writable: bool = True
        leaf: bool = True
        model: bool = False
        deny_write: bool = False
        unicode_paths: bool = False
        isolated_workspace: bool = False
        baseline_target: bool = False
        many_entries: int = Field(default=0, ge=0, le=32)
        workspace_group: str | None = Field(default=None, pattern=r"^[a-f0-9]{24}$")

    class Control(StrictModel):
        agent_id: str = Field(pattern=r"^[a-f0-9-]{36}$")
        action: Literal["unknown", "block_preview", "block_start", "release", "disable"]

    class Provider:
        name = "offline-browser-missing-directory-restore"

        def __init__(self):
            self.calls, self.target = 0, None

        async def stream(self, messages, _tools):
            self.calls += 1
            if self.calls == 1:
                yield ModelToolCall(
                    id="browser-restore-preview",
                    name="workspace_missing_directory_checkpoint_restore_preview",
                    arguments={
                        "target": {"kind": "checkpoint", "identifier": self.target},
                        "request_key": "browser-model-preview",
                    },
                )
            elif self.calls == 2:
                value = json.loads(messages[-1].content)
                assert value["phase"] == "ready", value
                yield ModelToolCall(
                    id="browser-restore-start",
                    name="workspace_missing_directory_checkpoint_restore_start",
                    arguments={
                        "plan_id": value["plan_id"],
                        "expected_revision": value["revision"],
                        "request_key": "browser-model-execute",
                    },
                )
            else:
                assert json.loads(messages[-1].content)["phase"] == "finished"
                yield ModelText(text="原模型目录恢复已完成")

        async def close(self):
            pass

    @app.post("/_test/missing-directory-restore-agent")
    async def actor(payload: Actor):
        group = payload.workspace_group or payload.name
        parent = root / ("missing-" + group)
        parent.mkdir(exist_ok=True)
        tree = parent / payload.name
        if tree.exists():
            raise ServiceError("test_restore_exists", "测试目录已存在", 409)
        (tree / "sub").mkdir(parents=True)
        (tree / "empty-dir").mkdir()
        (tree / "text.txt").write_bytes(b"original directory text\n")
        (tree / "sub/raw.bin").write_bytes(b"\0\xff")
        (tree / "sub/raw.bin").chmod(0o640)
        (tree / "empty.bin").write_bytes(b"")
        for index in range(payload.many_entries):
            (tree / f"page-{index:03d}.bin").write_bytes(b"\0\xff")
        if payload.unicode_paths:
            for name in ("\ue000.bin", "\U00010000.bin"):
                (tree / name).write_bytes(b"original unicode\n")
        # 每个测试独立物理父目录；同一测试的多个Actor仍共享资源，保留跨Scope断言。
        workspace = "missing_" + group
        if workspace not in workspace_groups:
            service.register_workspace(
                workspace, parent, version="restore-browser-v1", writable=payload.writable
            )
            workspace_groups.add(workspace)
        names = ["workspace_directory_checkpoint_capture"]
        if payload.baseline_target:
            names += list(DIRECTORY_RESTORE_STEP_TOOLS.values())
        if payload.leaf:
            names += [
                MISSING_DIRECTORY_RESTORE_STEP_TOOLS["prepare"],
                MISSING_DIRECTORY_RESTORE_STEP_TOOLS["verify"],
            ]
            if payload.writable:
                names.append(MISSING_DIRECTORY_RESTORE_STEP_TOOLS["execute"])
        provider_key = "runtime"
        provider = None
        if payload.model:
            names += sorted(WORKSPACE_MISSING_DIRECTORY_RESTORE_CONTROL_TOOL_NAMES)
            provider_key = "rp_" + payload.name
            providers[payload.name] = provider = Provider()
            service.register_provider(
                provider_key, provider, version="restore-browser-v1", owned=True
            )
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name=payload.name,
                    description="真实浏览器完整目录恢复",
                    workspace_key=workspace,
                    tool_names=names,
                    mode=payload.mode,
                    provider_key=provider_key,
                ),
            )
        )
        value = await service.spawn(payload.name)
        agent = AgentSession(service, value.agent_id)
        roots[value.agent_id] = tree
        checkpoint = None
        full_plan = None
        if payload.writable and payload.mode != "ask":
            checkpoint = await agent.create_directory_checkpoint(
                DirectoryCheckpointCreate(root_path=payload.name, request_key="target")
            )
            checkpoint = await agent.wait_directory_checkpoint(
                checkpoint.checkpoint_id, timeout_seconds=10
            )
            assert checkpoint.status == "available", checkpoint
            if provider:
                provider.target = checkpoint.checkpoint_id
            if payload.baseline_target:
                (tree / "sub/raw.bin").write_bytes(b"from original full baseline\0\x80")
                full_plan = await agent.preview_directory_checkpoint_restore(
                    DirectoryRestorePreview(
                        target=DirectoryRestoreTarget(identifier=checkpoint.checkpoint_id),
                        request_key="full-baseline-source",
                    )
                )
                full_plan = await agent.wait_directory_checkpoint_restore(
                    full_plan.plan_id, timeout_seconds=10
                )
                assert full_plan.phase == "ready" and full_plan.backup is not None
            tree.rename(parent / (".retained-" + payload.name))
        if payload.deny_write:
            handle = service._handle(value.agent_id)
            original_policy = handle.runtime.tool_policy

            async def deny(definition, context):
                if definition.name == MISSING_DIRECTORY_RESTORE_STEP_TOOLS["execute"]:
                    return "deny"
                return await original_policy(definition, context)

            handle.runtime.tool_policy = deny
        return {
            "agent": value.model_dump(mode="json"),
            "checkpoint": checkpoint.model_dump(mode="json") if checkpoint else None,
            "full_plan": full_plan.model_dump(mode="json") if full_plan else None,
        }

    @app.post("/_test/missing-directory-restore-control")
    async def control(payload: Control):
        if payload.action == "unknown":
            uncertain.add(payload.agent_id)
        elif payload.action in {"block_preview", "block_start"}:
            gates[payload.agent_id] = (
                "prepare" if payload.action == "block_preview" else "execute",
                asyncio.Event(),
                asyncio.Event(),
            )
        elif payload.action == "release":
            entry = gates.pop(payload.agent_id, None)
            if entry:
                entry[2].set()
        elif payload.action == "disable":
            session = await service.session(payload.agent_id)
            service.disable_workspace(session.permissions["workspace"]["key"])
        return {"controlled": True}

    @app.get("/_test/missing-directory-restore-proof/{agent_id}")
    async def proof(agent_id: str):
        if os.environ.get("KRIM_REQUIRE_INSTALLED_PACKAGE") == "1":
            assert "/site-packages/" in str(krim_agent.__file__)
        session = await service.session(agent_id)
        entries = await service.missing_directory_restore_store.list(
            "session:" + session.session_id
        )
        tree = roots[agent_id]
        provider = providers.get((await service.get(agent_id)).name)
        origins = []
        for value in entries:
            item = {"plan_id": value.plan_id}
            for phase in ("preview", "execute", "verify"):
                origin = await service.missing_directory_restore_store.source(
                    value.scope_id, value.plan_id, phase
                )
                item[phase] = (
                    {"run_id": origin.run_id, "call_id": origin.call_id} if origin else None
                )
            origins.append(item)
        return {
            "package": str(krim_agent.__file__),
            "counts": counts.get(agent_id, {}),
            "provider_calls": provider.calls if provider else 0,
            "plans": [value.model_dump(mode="json") for value in entries],
            "sources": origins,
            "files": {
                str(path.relative_to(tree.parent)): {
                    "hex": path.read_bytes().hex(),
                    "mode": path.stat().st_mode & 0o777,
                    "inode": path.stat().st_ino,
                }
                for path in tree.rglob("*")
                if path.is_file()
            },
            "root_inode": tree.stat().st_ino if tree.exists() else None,
        }
