"""纯 JSON 内建节点；外部工具、模型和代码交给可信 runner。"""

from __future__ import annotations

from collections.abc import Mapping

from jsonschema import Draft202012Validator
from pydantic import JsonValue

from krim_agent.protocol.models import check_json

from .types import NodeOutput, WorkflowError, WorkflowNode

PURE_KINDS = {
    "input",
    "output",
    "constant",
    "set",
    "branch",
    "switch",
    "aggregate",
    "validator",
    "enumerator",
    "image_preview",
    "model_preview",
}
NESTED_KINDS = {"loop", "fanout", "retry", "sub_workflow"}


def input_value(inputs: Mapping[str, JsonValue]) -> JsonValue:
    if set(inputs) == {"in"}:
        return inputs["in"]
    return dict(inputs)


def path_value(value: JsonValue, path: JsonValue) -> JsonValue:
    if path is None or path == "":
        return value
    if not isinstance(path, str):
        raise WorkflowError("invalid_path", "path 必须为点分隔字符串")
    current = value
    for key in path.split("."):
        if isinstance(current, dict) and key in current:
            current = current[key]
        elif isinstance(current, list) and key.isdigit() and int(key) < len(current):
            current = current[int(key)]
        else:
            raise WorkflowError("path_missing", f"输入路径不存在: {path}")
    return current


def condition(value: JsonValue, config: Mapping[str, JsonValue]) -> bool:
    selected = path_value(value, config.get("path"))
    expected = config.get("value")
    operator = config.get("operator", "truthy")
    if operator == "truthy":
        return bool(selected)
    if operator == "eq":
        return selected == expected
    if operator == "ne":
        return selected != expected
    if operator == "in":
        return isinstance(expected, list) and selected in expected
    if operator in {"gt", "gte", "lt", "lte"}:
        if (
            not isinstance(selected, (int, float))
            or isinstance(selected, bool)
            or not isinstance(expected, (int, float))
            or isinstance(expected, bool)
        ):
            raise WorkflowError("predicate_type", "比较操作要求两个有限数值")
        return {
            "gt": selected > expected,
            "gte": selected >= expected,
            "lt": selected < expected,
            "lte": selected <= expected,
        }[str(operator)]
    raise WorkflowError("unknown_predicate", f"不支持的比较操作: {operator}")


def run_builtin(
    node: WorkflowNode, inputs: dict[str, JsonValue], external_input: JsonValue
) -> NodeOutput:
    value = input_value(inputs)
    kind, config = node.kind, node.config
    selection: str | None = None
    if kind == "input":
        value = path_value(external_input, config.get("path"))
    elif kind == "constant":
        if "value" not in config:
            raise WorkflowError("invalid_config", "constant 必须配置 value")
        value = config["value"]
    elif kind == "set":
        additions = config.get("values", {})
        if not isinstance(additions, dict):
            raise WorkflowError("invalid_config", "set.values 必须为对象")
        merged: dict[str, JsonValue] = dict(value) if isinstance(value, dict) else {"input": value}
        merged.update(additions)
        value = merged
    elif kind == "branch":
        selection = "true" if condition(value, config) else "false"
    elif kind == "switch":
        selected = path_value(value, config.get("path"))
        if isinstance(selected, (dict, list)):
            raise WorkflowError("switch_type", "switch 路由值必须为标量")
        selection = (
            "null"
            if selected is None
            else str(selected).lower()
            if isinstance(selected, bool)
            else str(selected)
        )
    elif kind == "aggregate":
        items = value if isinstance(value, list) else list(inputs.values())
        mode = config.get("mode", "list")
        if mode == "list":
            value = items
        elif mode == "sum":
            if any(not isinstance(item, (int, float)) or isinstance(item, bool) for item in items):
                raise WorkflowError("aggregate_type", "sum 只能聚合有限数字")
            value = sum(item for item in items if isinstance(item, (int, float)))
        elif mode == "concat":
            if any(not isinstance(item, list) for item in items):
                raise WorkflowError("aggregate_type", "concat 要求数组输入")
            value = [child for item in items if isinstance(item, list) for child in item]
        elif mode == "merge":
            merged = {}
            for item in items:
                if not isinstance(item, dict):
                    raise WorkflowError("aggregate_type", "merge 要求对象输入")
                if any(key in merged for key in item):
                    raise WorkflowError("aggregate_collision", "merge 的输入键重复")
                merged.update(item)
            value = merged
        else:
            raise WorkflowError("invalid_config", "未知 aggregate.mode")
    elif kind == "enumerator":
        if not isinstance(value, list):
            raise WorkflowError("enumerator_type", "enumerator 要求数组")
        value = [{"index": index, "value": item} for index, item in enumerate(value)]
    elif kind == "validator":
        schema = config.get("schema", {})
        if not isinstance(schema, dict):
            raise WorkflowError("invalid_config", "validator.schema 必须为对象")
        Draft202012Validator.check_schema(schema)
        errors = list(Draft202012Validator(schema).iter_errors(value))
        if errors:
            raise WorkflowError("validation_error", "validator 输入不符合声明 Schema")
    elif kind not in {"output", "image_preview", "model_preview"}:
        raise WorkflowError("runner_required", f"节点 {kind} 需要可信 runner")
    check_json(value, max_bytes=131072)
    return NodeOutput(ports={"out": value}, selection=selection)
