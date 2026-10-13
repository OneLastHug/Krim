"""原生成根浏览器的合成控制与离线Model；产品不包含这些路由。"""

import json
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import Field, model_validator
from sqlalchemy import func, select

from krim_agent.agents.permissions import profile_policy
from krim_agent.generated_tree_undo import (
    GeneratedTreeUndoPreviewRequest,
    GeneratedTreeUndoPreviewView,
)
from krim_agent.persistence.database import (
    WorkspaceGeneratedTreeUndoRow,
    WorkspaceTreeMoveAttemptRow,
    WorkspaceTreeMoveModelSourceRow,
    WorkspaceTreePlanRow,
    WorkspaceTreeSealRow,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import StrictModel
from krim_agent.providers import ModelText, ModelToolCall, ModelUsage
from krim_agent.sdk import AgentSession
from krim_agent.usage import TokenUsage
from krim_agent.workspace_tree_move import TreeMoveInspectRequest


class RootControl(StrictModel):
    action: Literal["stat", "counts", "mutate", "catalog", "reject_next"]
    path: str = Field(default="tree", min_length=1, max_length=512)
    agent_id: str = Field(pattern=r"^[a-f0-9-]{36}$")
    plan_id: str | None = Field(default=None, pattern=r"^[a-f0-9-]{36}$")

    @model_validator(mode="after")
    def confined(self):
        TreeMoveInspectRequest(path=self.path)
        return self


class RootModel(StrictModel):
    agent_id: str = Field(pattern=r"^[a-f0-9-]{36}$")
    request: GeneratedTreeUndoPreviewRequest


def configure_generated_roots(app, runtime, root: Path) -> None:
    provider_calls: dict[str, int] = {}
    reject_next: set[str] = set()
    store = app.state.agents.manager.tree_moves.store
    original_authority = store.assert_execution

    async def authority(scope, identifier):
        await original_authority(scope, identifier)
        if scope in reject_next:
            reject_next.remove(scope)
            raise ServiceError("synthetic_root_history_rejection", "合成最终资格拒绝", 409)

    store.assert_execution = authority

    @app.post("/_test/generated-root")
    async def control(payload: RootControl) -> dict:
        agent = AgentSession(app.state.agents, payload.agent_id)
        target = root / payload.path
        if payload.action == "reject_next":
            reject_next.add("session:" + payload.agent_id)
            return {"scheduled": True}
        if payload.action == "catalog":
            inspection = await agent.inspect_exchange_tree(payload.path)
            identifiers = []
            for position in range(21):
                proposal = await agent.preview_generated_tree_undo(
                    GeneratedTreeUndoPreviewRequest(
                        plan_id=payload.plan_id,
                        preserve_path=f"{payload.path}-retained-{position}",
                        expected_fingerprint=inspection.fingerprint,
                        request_key=str(uuid4()),
                    ),
                    confirmed=True,
                )
                identifiers.append(proposal.move.proposal_id)
            return {"proposal_ids": identifiers}
        if payload.action == "mutate":
            (target / "sub/data.bin").write_bytes(b"changed")
        if payload.action == "counts":
            scope = "session:" + payload.agent_id
            async with runtime.repository.database.sessions() as sql:
                proposals = await sql.scalar(
                    select(func.count())
                    .select_from(WorkspaceGeneratedTreeUndoRow)
                    .where(WorkspaceGeneratedTreeUndoRow.scope_id == scope)
                )
                operations = await sql.scalar(
                    select(func.count())
                    .select_from(WorkspaceTreeMoveAttemptRow)
                    .join(
                        WorkspaceGeneratedTreeUndoRow,
                        WorkspaceTreeMoveAttemptRow.proposal_id
                        == WorkspaceGeneratedTreeUndoRow.proposal_id,
                    )
                    .where(WorkspaceGeneratedTreeUndoRow.scope_id == scope)
                )
                seals = await sql.scalar(
                    select(func.count())
                    .select_from(WorkspaceTreeSealRow)
                    .where(WorkspaceTreeSealRow.scope_id == scope)
                )
                plan_ids = list(
                    await sql.scalars(
                        select(WorkspaceTreePlanRow.plan_id).where(
                            WorkspaceTreePlanRow.scope_id == scope
                        )
                    )
                )
                sources = await sql.scalar(
                    select(func.count())
                    .select_from(WorkspaceTreeMoveModelSourceRow)
                    .join(
                        WorkspaceGeneratedTreeUndoRow,
                        WorkspaceTreeMoveModelSourceRow.proposal_id
                        == WorkspaceGeneratedTreeUndoRow.proposal_id,
                    )
                    .where(WorkspaceGeneratedTreeUndoRow.scope_id == scope)
                )
            plans = [
                (await app.state.agents.workspace_tree_store.get(scope, plan_id)).status
                for plan_id in plan_ids
            ]
            return {
                "proposals": proposals,
                "operations": operations,
                "seals": seals,
                "plans": plans,
                "sources": sources,
                "provider_calls": provider_calls.get(payload.agent_id, 0),
            }
        binary = target / "sub/data.bin"
        return {
            "found": target.exists(),
            "inode": target.stat().st_ino if target.exists() else None,
            "binary_inode": binary.stat().st_ino if binary.exists() else None,
            "binary_hex": binary.read_bytes().hex() if binary.exists() else None,
        }

    @app.post("/_test/model-generated-root")
    async def model(payload: RootModel) -> dict:
        agents = app.state.agents
        handle = agents._handle(payload.agent_id)
        previous_provider, previous_policy = handle.runtime.provider, handle.runtime.tool_policy
        change_key = str(uuid4())

        class Provider:
            name = "offline-browser-generated-root"
            calls = 0
            proposal: GeneratedTreeUndoPreviewView | None = None

            async def stream(self, messages, tools):
                self.calls += 1
                previous = json.loads(messages[-1].content) if self.calls > 1 else None
                if self.calls == 1:
                    name, arguments = (
                        "generated_tree_undo_preview",
                        payload.request.model_dump(mode="json"),
                    )
                elif self.calls == 2:
                    self.proposal = GeneratedTreeUndoPreviewView.model_validate_json(
                        json.dumps(previous)
                    )
                    name, arguments = (
                        "generated_tree_undo_change",
                        {
                            "proposal_id": self.proposal.move.proposal_id,
                            "action": "remove_root",
                            "expected_revision": self.proposal.move.revision,
                            "request_key": change_key,
                        },
                    )
                elif self.calls == 3:
                    assert previous["operation"]["status"] == "done", previous
                    name, arguments = (
                        "generated_tree_undo_query",
                        {"kind": "proposal", "identifier": self.proposal.move.proposal_id},
                    )
                else:
                    assert previous["proposal"]["root_state"] == "preserved", previous
                    yield ModelText(text="原模型生成根已保留")
                    yield ModelUsage(
                        usage=TokenUsage(input_tokens=10, output_tokens=1, total_tokens=11)
                    )
                    return
                yield ModelToolCall(
                    id=f"browser-generated-root-{self.calls}", name=name, arguments=arguments
                )
                yield ModelUsage(
                    usage=TokenUsage(input_tokens=10, output_tokens=1, total_tokens=11)
                )

        async def allow(_definition, _context):
            return "allow"

        provider = Provider()
        handle.runtime.provider = provider
        handle.runtime.tool_policy = profile_policy(handle.definition.mode, allow)
        try:
            agent = AgentSession(agents, payload.agent_id)
            result = await agent.execute("原模型生成根浏览器冷读取", idempotency_key=str(uuid4()))
            assert result.status == "completed", result
            provider_calls[payload.agent_id] = provider.calls
            async with runtime.repository.database.sessions() as sql:
                sources = [
                    (row.run_id, row.call_id)
                    for row in await sql.scalars(
                        select(WorkspaceTreeMoveModelSourceRow).where(
                            WorkspaceTreeMoveModelSourceRow.proposal_id
                            == provider.proposal.move.proposal_id
                        )
                    )
                ]
            assert set(sources) == {
                (result.run_id, "browser-generated-root-1"),
                (result.run_id, "browser-generated-root-2"),
            }
            return {
                "run_id": result.run_id,
                "proposal_id": provider.proposal.move.proposal_id,
                "preview_key": payload.request.request_key,
                "change_key": change_key,
                "provider_calls": provider.calls,
            }
        finally:
            handle.runtime.provider, handle.runtime.tool_policy = previous_provider, previous_policy
