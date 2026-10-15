"""只在显式测试开关下注册离线Model；浏览器仍走公共执行/审批/历史接口。"""

import json

from pydantic import Field
from sqlalchemy import select

from krim_agent.agents import AgentProfile, AgentProfileUpdate
from krim_agent.persistence.database import (
    NativeCheckpointModelSourceRow,
    NativeWorkspaceCheckpointRow,
)
from krim_agent.protocol.models import StrictModel
from krim_agent.providers import ModelText, ModelToolCall, ModelUsage
from krim_agent.usage import TokenUsage


def configure_model_checkpoints(app, root):
    service = app.state.agents
    service.register_checkpoint_controls()
    (root / "model-plain.txt").write_text("模型真实快照\n")
    (root / "model-binary.bin").write_bytes(b"\0\xff")
    providers = {}

    class Body(StrictModel):
        name: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,39}$")
        capture_format: str = Field(pattern=r"^(text|bytes)$")

    class Provider:
        name = "browser-offline-checkpoint-model"

        def __init__(self, mode, key):
            self.mode, self.key, self.calls, self.checkpoint_id = mode, key, 0, None

        async def stream(self, messages, tools):
            self.calls += 1
            if self.calls == 1:
                paths = ["model-plain.txt", "model-absent.bin"]
                if self.mode == "bytes":
                    paths.insert(1, "model-binary.bin")
                yield ModelToolCall(
                    id="browser-model-create",
                    name="workspace_checkpoint_create",
                    arguments={
                        "title": "原模型检查点",
                        "paths": paths,
                        "request_key": self.key,
                        "capture_format": self.mode,
                    },
                )
            elif self.calls == 2:
                view = json.loads(messages[-1].content)
                assert view["status"] == "available"
                self.checkpoint_id = view["checkpoint_id"]
                yield ModelToolCall(
                    id="browser-model-query",
                    name="workspace_checkpoint_query",
                    arguments={
                        "kind": "id",
                        "checkpoint_id": self.checkpoint_id,
                    },
                )
            else:
                assert (
                    json.loads(messages[-1].content)["checkpoint"]["checkpoint_id"]
                    == self.checkpoint_id
                )
                yield ModelText(text="原模型检查点已创建并核实")
                yield ModelUsage(usage=TokenUsage(input_tokens=5, output_tokens=1, total_tokens=6))

        async def close(self):
            pass

    @app.post("/_test/model-checkpoint-agent")
    async def actor(payload: Body):
        provider = Provider(payload.capture_format, payload.name + "-root")
        key = "cp_" + payload.name
        service.register_provider(key, provider, version="browser-model-v1", owned=True)
        await service.profile_put(
            AgentProfileUpdate(
                expected_revision=0,
                config=AgentProfile(
                    name=payload.name,
                    description="显式离线Model检查点",
                    provider_key=key,
                    workspace_key="edit",
                    tool_timeout_seconds=30,
                    tool_names=[
                        "workspace_read",
                        "workspace_checkpoint_capture",
                        "workspace_checkpoint_create",
                        "workspace_checkpoint_query",
                        "workspace_checkpoint_cancel",
                    ],
                ),
            )
        )
        view = await service.spawn(payload.name)
        providers[view.agent_id] = provider
        return view

    @app.get("/_test/model-checkpoint-proof/{agent_id}")
    async def proof(agent_id: str):
        provider = providers[agent_id]
        async with service.workspace_edits.database.sessions() as sql:
            rows = list(
                await sql.scalars(
                    select(NativeWorkspaceCheckpointRow).where(
                        NativeWorkspaceCheckpointRow.scope_id == "session:" + agent_id
                    )
                )
            )
            sources = []
            for row in rows:
                source = await sql.get(NativeCheckpointModelSourceRow, row.checkpoint_id)
                sources.append(
                    {
                        "checkpoint_id": row.checkpoint_id,
                        "run_id": source.run_id,
                        "call_id": source.call_id,
                        "model_bound": row.model_bound,
                    }
                )
        return {
            "calls": provider.calls,
            "sources": sources,
            "checkpoint_id": provider.checkpoint_id,
        }
