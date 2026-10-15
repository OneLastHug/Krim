"""显式测试开关下的离线模型恢复；浏览器读取原持久事实。"""

import hashlib
import json

from pydantic import Field
from sqlalchemy import select

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.persistence.database import (
    NativeCheckpointModelSourceRow,
    WorkspaceRestoreModelSourceRow,
)
from krim_agent.protocol.models import StrictModel
from krim_agent.providers import ModelText, ModelToolCall


def configure_model_restores(app, root):
    service = app.state.agents
    service.register_checkpoint_controls()
    service.register_checkpoint_restore_controls()
    providers = {}

    class Body(StrictModel):
        name: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,39}$")
        capture_format: str = Field(pattern=r"^(text|bytes)$")

    class Provider:
        name = "browser-offline-model-restore"

        def __init__(self, name, capture_format):
            self.calls = 0
            self.mode = capture_format
            self.paths = [name + "-plain", name + "-absent"]
            if self.mode == "bytes":
                self.paths.insert(1, name + "-binary")
            (root / self.paths[0]).write_text("target\n")
            (root / self.paths[0]).chmod(0o640)
            if self.mode == "bytes":
                (root / self.paths[1]).write_bytes(b"\x00\xff")
            self.target = self.plan = self.done = None

        async def stream(self, messages, tools):
            self.calls += 1
            if self.calls == 1:
                yield ModelToolCall(
                    id="browser-model-target",
                    name="workspace_checkpoint_create",
                    arguments={
                        "paths": self.paths,
                        "capture_format": self.mode,
                        "request_key": "target",
                    },
                )
            elif self.calls == 2:
                self.target = json.loads(messages[-1].content)
                assert self.target["status"] == "available"
                (root / self.paths[0]).write_text("current\n")
                (root / self.paths[0]).chmod(0o600)
                (root / self.paths[-1]).write_text("new\n")
                if self.mode == "bytes":
                    (root / self.paths[1]).write_bytes(b"changed\x00\xff")
                yield ModelToolCall(
                    id="browser-model-preview",
                    name="workspace_checkpoint_restore_preview",
                    arguments={
                        "checkpoint_id": self.target["checkpoint_id"],
                        "request_key": "preview",
                    },
                )
            elif self.calls == 3:
                self.plan = json.loads(messages[-1].content)
                assert self.plan["phase"] == "ready"
                yield ModelToolCall(
                    id="browser-model-start",
                    name="workspace_checkpoint_restore_start",
                    arguments={
                        "plan_id": self.plan["plan_id"],
                        "expected_revision": self.plan["revision"],
                        "request_key": "start",
                    },
                )
            elif self.calls == 4:
                self.done = json.loads(messages[-1].content)
                assert self.done["status"] == "done"
                yield ModelToolCall(
                    id="browser-model-query",
                    name="workspace_checkpoint_restore_query",
                    arguments={
                        "kind": "id",
                        "plan_id": self.done["plan_id"],
                    },
                )
            else:
                assert self.calls == 5 and json.loads(messages[-1].content)["plan"] == self.done
                yield ModelText(text="原模型恢复已完成并核实")

        async def close(self):
            pass

    @app.post("/_test/model-restore-agent")
    async def actor(payload: Body):
        provider = Provider(payload.name, payload.capture_format)
        key = "mr_" + payload.name
        service.register_provider(key, provider, version="browser-model-v1", owned=True)
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name=payload.name,
                    description="原模型恢复",
                    provider_key=key,
                    workspace_key="edit",
                    tool_timeout_seconds=30,
                    tool_names=[
                        "workspace_read",
                        "workspace_checkpoint_capture",
                        "workspace_change",
                        "workspace_reconcile",
                        "workspace_checkpoint_create",
                        "workspace_checkpoint_query",
                        "workspace_checkpoint_cancel",
                        "workspace_checkpoint_restore_preview",
                        "workspace_checkpoint_restore_start",
                        "workspace_checkpoint_restore_query",
                        "workspace_checkpoint_restore_cancel",
                    ],
                ),
            )
        )
        view = await service.spawn(payload.name)
        providers[view.agent_id] = provider
        return view

    @app.get("/_test/model-restore-proof/{agent_id}")
    async def proof(agent_id: str):
        provider = providers[agent_id]
        async with service.workspace_edits.database.sessions() as sql:
            rows = list(
                await sql.scalars(
                    select(WorkspaceRestoreModelSourceRow).where(
                        WorkspaceRestoreModelSourceRow.plan_id == provider.done["plan_id"]
                    )
                )
            )
            baseline = await sql.get(
                NativeCheckpointModelSourceRow, provider.done["baseline_checkpoint_id"]
            )
        return {
            "calls": provider.calls,
            "plan": provider.done,
            "sources": [
                {"phase": row.phase, "run_id": row.run_id, "call_id": row.call_id} for row in rows
            ],
            "baseline_source": {
                "run_id": baseline.run_id,
                "call_id": baseline.call_id,
                "restore_plan_id": baseline.restore_plan_id,
            },
            "files": {
                name: {
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    "mode": path.stat().st_mode & 0o777,
                }
                if (path := root / name).exists()
                else None
                for name in provider.paths
            },
        }
