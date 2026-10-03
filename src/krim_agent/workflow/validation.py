"""Workflow 静态校验：端点、端口、唯一边、DAG、组和生产投影。"""

from __future__ import annotations

from collections import defaultdict

from .schema import compatible, schema_valid
from .types import WorkflowEdge, WorkflowError, WorkflowGraph


def validate_graph(graph: WorkflowGraph) -> None:
    nodes = graph.node_map()
    adjacency: dict[str, list[str]] = defaultdict(list)
    incoming: dict[str, list[WorkflowEdge]] = defaultdict(list)
    input_ports = {node.id: {port.name: port for port in node.inputs()} for node in graph.nodes}
    output_ports = {node.id: {port.name: port for port in node.outputs()} for node in graph.nodes}
    schema_valid(graph.input_schema)
    schema_valid(graph.output_schema)
    for node in graph.nodes:
        if len(input_ports[node.id]) != len(node.inputs()) or len(output_ports[node.id]) != len(
            node.outputs()
        ):
            raise WorkflowError("duplicate_port", f"节点端口重复: {node.id}", node_id=node.id)
        for port in [*node.inputs(), *node.outputs()]:
            schema_valid(port.json_schema)
        if node.kind in {"image_preview", "model_preview"} and any(
            edge.source == node.id for edge in graph.edges
        ):
            raise WorkflowError("preview_not_sink", "预览节点必须是 sink", node_id=node.id)
    seen_edges: set[tuple[str, str, str, str, str | None]] = set()
    for edge in graph.edges:
        if edge.source not in nodes or edge.target not in nodes:
            raise WorkflowError("edge_endpoint", "边引用了不存在的节点")
        key = (edge.source, edge.source_port, edge.target, edge.target_port, edge.when)
        if key in seen_edges:
            raise WorkflowError("duplicate_edge", "重复边不能出现在图中")
        seen_edges.add(key)
        if (
            edge.source_port not in output_ports[edge.source]
            or edge.target_port not in input_ports[edge.target]
        ):
            raise WorkflowError("port_missing", f"边端口不存在: {edge.source}->{edge.target}")
        source_schema = output_ports[edge.source][edge.source_port].json_schema
        target = input_ports[edge.target][edge.target_port]
        target_schema = target.json_schema
        item_schema = target_schema.get("items")
        if target.multiple and isinstance(item_schema, dict):
            target_schema = item_schema
        elif target.multiple:
            target_schema = {}
        if not compatible(source_schema, target_schema):
            raise WorkflowError("port_type", f"端口类型不兼容: {edge.source}->{edge.target}")
        if edge.when is not None and nodes[edge.source].kind not in {
            "branch",
            "switch",
            "classifier",
        }:
            raise WorkflowError("invalid_conditional_edge", "条件边只能来自路由节点")
        adjacency[edge.source].append(edge.target)
        incoming[edge.target].append(edge)
    color = {node.id: 0 for node in graph.nodes}

    def visit(identifier: str) -> None:
        color[identifier] = 1
        for target in adjacency[identifier]:
            if color[target] == 1:
                raise WorkflowError("cycle", "Workflow 图必须是 DAG；循环请使用 loop 节点")
            if color[target] == 0:
                visit(target)
        color[identifier] = 2

    for identifier in color:
        if color[identifier] == 0:
            visit(identifier)
    sinks = [node.id for node in graph.nodes if not adjacency[node.id]]
    if not sinks:
        raise WorkflowError("no_sink", "Workflow 必须至少有一个终点")
    if graph.output_node is not None and graph.output_node not in sinks:
        raise WorkflowError("invalid_output", "output_node 必须是终点节点")
    if len({group.id for group in graph.groups}) != len(graph.groups):
        raise WorkflowError("duplicate_group", "Group ID 重复")
    for group in graph.groups:
        if len(group.node_ids) != len(set(group.node_ids)):
            raise WorkflowError("duplicate_group_node", f"Group 节点重复: {group.id}")
        if any(identifier not in nodes for identifier in group.node_ids):
            raise WorkflowError("group_node_missing", f"Group 引用了不存在节点: {group.id}")
    for target_id, edges in incoming.items():
        ports_for_target = {port.name: port for port in nodes[target_id].inputs()}
        counts: dict[str, int] = defaultdict(int)
        for edge in edges:
            counts[edge.target_port] += 1
        for port_name, count in counts.items():
            if count > 1 and not ports_for_target[port_name].multiple:
                raise WorkflowError(
                    "port_fan_in", f"单值端口不能接收多条边: {target_id}.{port_name}"
                )
