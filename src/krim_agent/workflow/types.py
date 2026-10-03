"""Krim Workflow JSON 合约；命名端口、图、运行快照和有序事件。"""

from __future__ import annotations

from typing import Literal
from uuid import uuid4

from pydantic import Field, JsonValue, field_validator, model_validator

from krim_agent.protocol.models import StrictModel, check_json, utc_now

NodeKind = Literal[
    "input",
    "output",
    "constant",
    "set",
    "branch",
    "switch",
    "loop",
    "aggregate",
    "fanout",
    "retry",
    "agent",
    "classifier",
    "summary",
    "validator",
    "enumerator",
    "tool",
    "code",
    "sub_workflow",
    "image_preview",
    "model_preview",
]
WorkflowStatus = Literal["queued", "running", "paused", "completed", "failed", "aborted"]
NodeStatus = Literal["pending", "running", "completed", "skipped", "failed", "aborted"]
EventType = Literal[
    "start",
    "node_start",
    "node_end",
    "node_skip",
    "node_retry",
    "paused",
    "resumed",
    "rerun",
    "end",
    "error",
]


class WorkflowError(Exception):
    def __init__(self, code: str, message: str, *, node_id: str | None = None) -> None:
        super().__init__(message)
        self.code, self.node_id = code, node_id


class WorkflowPort(StrictModel):
    name: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
    json_schema: dict[str, JsonValue] = Field(default_factory=dict)
    required: bool = True
    multiple: bool = False


class RetryPolicy(StrictModel):
    max_attempts: int = Field(default=1, ge=1, le=8)
    backoff_seconds: float = Field(default=0, ge=0, le=10)


class WorkflowNode(StrictModel):
    id: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
    kind: NodeKind
    config: dict[str, JsonValue] = Field(default_factory=dict)
    input_ports: list[WorkflowPort] | None = None
    output_ports: list[WorkflowPort] | None = None
    retry: RetryPolicy = Field(default_factory=RetryPolicy)
    timeout_seconds: float = Field(default=30, gt=0, le=300)
    developer_only: bool = False

    @field_validator("kind", mode="before")
    @classmethod
    def aliases(cls, value: object) -> object:
        return (
            {
                "subWorkflow": "sub_workflow",
                "imagePreview": "image_preview",
                "modelPreview": "model_preview",
            }.get(value, value)
            if isinstance(value, str)
            else value
        )

    @model_validator(mode="after")
    def finite_config(self) -> WorkflowNode:
        check_json(self.config)
        return self

    def inputs(self) -> list[WorkflowPort]:
        if self.input_ports is not None:
            return self.input_ports
        if self.kind in {"constant", "input"}:
            return []
        if self.kind == "aggregate":
            return [WorkflowPort(name="in", multiple=True, json_schema={"type": "array"})]
        return [WorkflowPort(name="in")]

    def outputs(self) -> list[WorkflowPort]:
        return self.output_ports if self.output_ports is not None else [WorkflowPort(name="out")]


class WorkflowEdge(StrictModel):
    source: str
    target: str
    source_port: str = "out"
    target_port: str = "in"
    when: str | None = None


class WorkflowGroup(StrictModel):
    id: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
    label: str = Field(default="", max_length=120)
    node_ids: list[str] = Field(default_factory=list)


class WorkflowGraph(StrictModel):
    nodes: list[WorkflowNode] = Field(min_length=1, max_length=256)
    edges: list[WorkflowEdge] = Field(default_factory=list, max_length=2048)
    groups: list[WorkflowGroup] = Field(default_factory=list, max_length=128)
    input_schema: dict[str, JsonValue] = Field(default_factory=dict)
    output_schema: dict[str, JsonValue] = Field(default_factory=dict)
    output_node: str | None = None

    def node_map(self) -> dict[str, WorkflowNode]:
        result = {node.id: node for node in self.nodes}
        if len(result) != len(self.nodes):
            raise WorkflowError("duplicate_node", "节点 ID 重复")
        return result

    def validate_graph(self) -> None:
        from .validation import validate_graph

        validate_graph(self)

    def derived_input_schema(self) -> dict[str, JsonValue]:
        if self.input_schema:
            return dict(self.input_schema)
        schemas: list[JsonValue] = []
        for node in self.nodes:
            if any(edge.target == node.id for edge in self.edges):
                continue
            ports = node.outputs() if node.kind == "input" else node.inputs()
            if len(ports) == 1 and ports[0].json_schema:
                schemas.append(ports[0].json_schema)
            elif len(ports) > 1:
                properties: dict[str, JsonValue] = {port.name: port.json_schema for port in ports}
                required: list[JsonValue] = [port.name for port in ports if port.required]
                schemas.append({"type": "object", "properties": properties, "required": required})
        return {"allOf": schemas} if schemas else {}

    def derived_output_schema(self) -> dict[str, JsonValue]:
        if self.output_schema:
            return dict(self.output_schema)
        candidates = (
            [node for node in self.nodes if node.id == self.output_node]
            if self.output_node
            else [
                node
                for node in self.nodes
                if not any(edge.source == node.id for edge in self.edges)
                and node.kind not in {"image_preview", "model_preview"}
            ]
        )
        if len(candidates) == 1 and len(candidates[0].outputs()) == 1:
            return dict(candidates[0].outputs()[0].json_schema)
        properties: dict[str, JsonValue] = {
            node.id: node.outputs()[0].json_schema if len(node.outputs()) == 1 else {}
            for node in candidates
        }
        scalar_schemas: list[JsonValue] = [
            node.outputs()[0].json_schema if len(node.outputs()) == 1 else {"type": "object"}
            for node in candidates
        ]
        return (
            {"anyOf": [*scalar_schemas, {"type": "object", "properties": properties}]}
            if candidates
            else {"type": "object"}
        )

    def visible_nodes(self) -> list[WorkflowNode]:
        return [node for node in self.nodes if not node.developer_only]

    def production_projection(self) -> WorkflowGraph:
        visible = {node.id for node in self.visible_nodes()}
        if not visible:
            raise WorkflowError("empty_production_graph", "生产图没有可执行节点")
        if any(edge.source not in visible and edge.target in visible for edge in self.edges):
            raise WorkflowError("developer_dependency", "生产节点依赖已剥离的开发节点")
        if self.output_node is not None and self.output_node not in visible:
            raise WorkflowError("developer_output", "生产图输出不能是开发节点")
        return self.model_copy(
            update={
                "nodes": [node.model_copy(deep=True) for node in self.nodes if node.id in visible],
                "edges": [
                    edge.model_copy(deep=True)
                    for edge in self.edges
                    if edge.source in visible and edge.target in visible
                ],
                "groups": [
                    group.model_copy(
                        update={
                            "node_ids": [
                                identifier for identifier in group.node_ids if identifier in visible
                            ]
                        }
                    )
                    for group in self.groups
                    if any(identifier in visible for identifier in group.node_ids)
                ],
            },
            deep=True,
        )


class NodeOutput(StrictModel):
    ports: dict[str, JsonValue] = Field(default_factory=dict)
    selection: str | None = None


class WorkflowEvent(StrictModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    run_id: str
    seq: int = Field(ge=0)
    epoch: int = Field(default=0, ge=0)
    type: EventType
    node_id: str | None = None
    group_ids: list[str] = Field(default_factory=list)
    data: JsonValue = None
    created_at: str = Field(default_factory=lambda: utc_now().isoformat())


class WorkflowResult(StrictModel):
    run_id: str
    status: WorkflowStatus
    output: JsonValue = None
    node_results: dict[str, JsonValue] = Field(default_factory=dict)
    node_outputs: dict[str, dict[str, JsonValue]] = Field(default_factory=dict)
    node_statuses: dict[str, NodeStatus] = Field(default_factory=dict)
    error: str | None = None
    code: str | None = None
    epoch: int = 0
