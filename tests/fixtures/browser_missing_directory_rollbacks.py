"""显式本机浏览器回退夹具；生产沿原SDK/Registry/SQL/物理树。"""

import sys
from pathlib import Path
from threading import Event
from typing import Literal

from pydantic import Field
from sqlalchemy import func, select

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.persistence.database import WorkspaceTreeMoveAttemptRow
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.sdk import AgentSession, DirectoryCheckpointCreate, MissingDirectoryRollbackQuery
from krim_agent.workspace_missing_directory_restore import (
    DirectoryRestoreTarget,
    MissingDirectoryRestoreExecute,
    MissingDirectoryRestorePreview,
)
from krim_agent.workspace_missing_directory_restore_control import (
    MISSING_DIRECTORY_RESTORE_STEP_TOOLS,
)
from krim_agent.workspace_tree_writer import WorkspaceTreeWriter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_directory_checkpoint_capture import make_tree  # noqa: E402
from test_missing_baseline_rollback_control import NAMES, Provider  # noqa: E402


def configure_missing_directory_rollbacks(app, root):
    service = app.state.agents
    service.register_directory_checkpoints()
    service.register_missing_directory_restores()
    service.register_missing_directory_rollbacks()
    roots, sources, applies, gates, providers, requests = {}, {}, {}, {}, {}, {}
    unknown = set()
    original_apply = WorkspaceTreeWriter.apply
    settle = service.manager.tree_moves.store.settle

    def apply(writer, prepared, **kwargs):
        path = writer.workspace.root
        agent_id = next((key for key, value in roots.items() if value == path), None)
        if agent_id is not None:
            applies[agent_id] = applies.get(agent_id, 0) + 1
            if agent_id in gates:
                gates[agent_id][0].set()
                assert gates[agent_id][1].wait(5), "测试须在原5秒内主动释放"
        return original_apply(writer, prepared, **kwargs)

    async def lost(scope, *args, **kwargs):
        if scope in unknown:
            unknown.remove(scope)
            raise OSError("合成效果后SQL回程中断")
        return await settle(scope, *args, **kwargs)

    WorkspaceTreeWriter.apply = apply
    service.manager.tree_moves.store.settle = lost

    class Actor(StrictModel):
        name: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,39}$")
        inspect: bool = True
        deny_write: bool = False
        readonly: bool = False
        model: bool = False

    @app.post("/_test/missing-rollback-agent")
    async def actor(payload: Actor):
        parent = root / ("missing-rollback-" + payload.name)
        parent.mkdir()
        make_tree(parent)
        workspace = "rb_" + payload.name
        binding = service.register_workspace(
            workspace, parent, version="rollback-browser-v1", writable=not payload.readonly
        )
        names = (
            ["missing_directory_rollback_query"]
            if payload.readonly
            else [
                "workspace_directory_checkpoint_capture",
                *MISSING_DIRECTORY_RESTORE_STEP_TOOLS.values(),
                *NAMES,
            ]
        )
        if payload.inspect and not payload.readonly:
            names.append("tree_move_inspect")
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name=payload.name,
                    description="真实Missing回退浏览器",
                    workspace_key=workspace,
                    tool_names=names,
                    tool_timeout_seconds=10,
                ),
            )
        )
        value = await service.spawn(payload.name)
        agent = AgentSession(service, value.agent_id)
        roots[value.agent_id] = parent
        finished = None
        if not payload.readonly:
            captured = await agent.create_directory_checkpoint(
                DirectoryCheckpointCreate(root_path="tree", request_key="target"), confirmed=True
            )
            target = await agent.wait_directory_checkpoint(
                captured.checkpoint_id, timeout_seconds=10
            )
            assert target.status == "available"
            (parent / "tree").rename(parent / "retained-original")
            planned = await agent.preview_missing_directory_checkpoint_restore(
                MissingDirectoryRestorePreview(
                    target=DirectoryRestoreTarget(identifier=target.checkpoint_id),
                    request_key="original-preview",
                ),
                confirmed=True,
            )
            planned = await agent.wait_missing_directory_checkpoint_restore(
                planned.plan_id, timeout_seconds=10
            )
            assert planned.phase == "ready"
            await agent.execute_missing_directory_checkpoint_restore(
                MissingDirectoryRestoreExecute(
                    plan_id=planned.plan_id,
                    expected_revision=planned.revision,
                    request_key="original-execute",
                ),
                confirmed=True,
            )
            finished = await agent.wait_missing_directory_checkpoint_restore(
                planned.plan_id, timeout_seconds=10
            )
            assert finished.phase == "finished", finished
            (parent / "tree/nested/nul").write_bytes(b"user\0modified\xff")
            (parent / "tree/user-extra").write_bytes(b"preserve me")
            sources[value.agent_id] = finished
        if payload.deny_write:
            policy = service._handle(value.agent_id).runtime.tool_policy

            async def deny(definition, context):
                return (
                    "deny"
                    if definition.name == "missing_directory_rollback_change"
                    else await policy(definition, context)
                )

            service._handle(value.agent_id).runtime.tool_policy = deny
        if payload.model:
            from krim_agent.workspace_missing_directory_rollback import (
                MissingDirectoryRollbackPreview,
            )
            from krim_agent.workspace_tree_snapshot import WorkspaceTreeSnapshotReader

            request = MissingDirectoryRollbackPreview(
                plan_id=finished.plan_id,
                preserve_path="preserved",
                expected_fingerprint=WorkspaceTreeSnapshotReader(binding.workspace)
                .capture("tree")
                .fingerprint,
                request_key="rollback-preview",
            )
            provider = providers[value.agent_id] = Provider(request)
            requests[value.agent_id] = request
            service._handle(value.agent_id).runtime.provider = provider

            async def allow(_definition, _context):
                return "allow"

            service._handle(value.agent_id).runtime.tool_policy = allow
            result = await agent.execute(
                "原模型完整保留并回到缺失", idempotency_key="original-model"
            )
            assert result.status == "completed", result
        return {
            "agent": value.model_dump(mode="json"),
            "source": finished.model_dump(mode="json") if finished else None,
        }

    class Control(StrictModel):
        agent_id: str
        action: Literal["unknown", "block", "release", "disable"]

    @app.post("/_test/missing-rollback-control")
    async def control(payload: Control):
        if payload.agent_id not in roots:
            raise ServiceError("test_agent_missing", "测试Agent不存在", 404)
        if payload.action == "unknown":
            unknown.add("session:" + payload.agent_id)
        elif payload.action == "block":
            gates[payload.agent_id] = (Event(), Event())
        elif payload.action == "release":
            gates[payload.agent_id][1].set()
        else:
            service.disable_workspace(service._handle(payload.agent_id).definition.workspace.key)
        return {"ok": True}

    @app.get("/_test/missing-rollback-proof/{agent_id}")
    async def proof(agent_id: str):
        parent = roots[agent_id]
        file = parent / ("tree" if (parent / "tree").is_dir() else "preserved") / "nested/nul"
        async with service.workspace_edits.database.sessions() as sql:
            attempts = await sql.scalar(
                select(func.count())
                .select_from(WorkspaceTreeMoveAttemptRow)
                .where(WorkspaceTreeMoveAttemptRow.scope_id == "session:" + agent_id)
            )
        source = sources.get(agent_id)
        if source is not None:
            assert (
                await service.workspace_missing_directory_restore(agent_id, source.plan_id)
                == source
            )
        return {
            "tree": (parent / "tree").is_dir(),
            "preserved": (parent / "preserved").is_dir(),
            "binary_hex": file.read_bytes().hex(),
            "mode": file.stat().st_mode & 0o777,
            "extra": (file.parents[1] / "user-extra").read_text()
            if (file.parents[1] / "user-extra").exists()
            else None,
            "applies": applies.get(agent_id, 0),
            "attempts": attempts,
            "provider_calls": providers[agent_id].calls if agent_id in providers else 0,
            "blocked": agent_id in gates and gates[agent_id][0].is_set(),
            "source_plan_id": source.plan_id if source else None,
        }

    @app.get("/_test/missing-rollback-original/{agent_id}")
    async def original(agent_id: str):
        return (
            await service.missing_directory_rollback_query(
                agent_id,
                MissingDirectoryRollbackQuery(kind="by_key", request_key="rollback-preview"),
            )
        ).model_dump(mode="json")
