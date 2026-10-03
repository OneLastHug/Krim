"""Workflow API 的请求/快照模型；不暴露 runner callable。"""

from __future__ import annotations

from pydantic import Field, JsonValue

from krim_agent.protocol.models import StrictModel

from .types import (
    NodeOutput,
    WorkflowCheckpointState,
    WorkflowGraph,
    WorkflowResult,
    WorkflowStatus,
)


class WorkflowStartRequest(StrictModel):
    idempotency_key: str = Field(min_length=1, max_length=128)
    graph: WorkflowGraph
    input_value: JsonValue = None
    production: bool = True
    pause_before: list[str] = Field(default_factory=list, max_length=256)
    pause_after: list[str] = Field(default_factory=list, max_length=256)


class WorkflowResumeRequest(StrictModel):
    has_input_override: bool = False
    input_override: JsonValue = None


class WorkflowRerunRequest(StrictModel):
    node_id: str = Field(min_length=1, max_length=64)
    has_input_override: bool = False
    input_override: JsonValue = None
    allow_side_effects: bool = False


class WorkflowRestoreRequest(StrictModel):
    replay_side_effects: bool = False
    completed_outputs: dict[str, NodeOutput] = Field(default_factory=dict)


class WorkflowRunView(StrictModel):
    run_id: str
    status: WorkflowStatus
    epoch: int
    current_node: str | None = None
    output: JsonValue = None
    node_results: dict[str, JsonValue] = Field(default_factory=dict)
    node_outputs: dict[str, dict[str, JsonValue]] = Field(default_factory=dict)
    node_statuses: dict[str, str] = Field(default_factory=dict)
    error: str | None = None
    code: str | None = None
    event_count: int = 0
    production: bool = True


def checkpoint_view(state: WorkflowCheckpointState) -> WorkflowRunView:
    result = state.result
    return WorkflowRunView(
        run_id=state.run_id,
        status=state.status,
        epoch=state.epoch,
        current_node=state.current_node,
        output=result.output if result is not None else None,
        node_results=result.node_results
        if result is not None
        else {
            identifier: value.ports.get("out", value.ports)
            if set(value.ports) == {"out"}
            else value.ports
            for identifier, value in state.node_outputs.items()
        },
        node_outputs={identifier: value.ports for identifier, value in state.node_outputs.items()},
        node_statuses={key: str(value) for key, value in state.node_statuses.items()},
        error=result.error if result is not None else None,
        code=result.code if result is not None else None,
        event_count=len(state.events),
        production=state.production,
    )


def result_view(result: WorkflowResult, *, event_count: int, production: bool) -> WorkflowRunView:
    return WorkflowRunView(
        run_id=result.run_id,
        status=result.status,
        epoch=result.epoch,
        output=result.output,
        node_results=result.node_results,
        node_outputs=result.node_outputs,
        node_statuses={key: str(value) for key, value in result.node_statuses.items()},
        error=result.error,
        code=result.code,
        event_count=event_count,
        production=production,
    )
