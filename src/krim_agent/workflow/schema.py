"""JSON Schema 校验和已知端口类型兼容；未知 Schema 在运行边界裁决。"""

from __future__ import annotations

from jsonschema import Draft202012Validator, SchemaError
from pydantic import JsonValue

from krim_agent.protocol.models import check_json

from .types import WorkflowError


def schema_valid(schema: dict[str, JsonValue]) -> None:
    check_json(schema)
    pending: list[JsonValue] = [schema]
    while pending:
        value = pending.pop()
        if isinstance(value, dict):
            reference = value.get("$ref")
            if reference is not None and (
                not isinstance(reference, str) or not reference.startswith("#")
            ):
                raise WorkflowError("external_schema_ref", "Workflow Schema 仅允许本地引用")
            pending.extend(value.values())
        elif isinstance(value, list):
            pending.extend(value)
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as error:
        raise WorkflowError("invalid_schema", "端口 JSON Schema 不合法") from error


def schema_accepts(
    value: JsonValue, schema: dict[str, JsonValue], *, node_id: str | None = None
) -> None:
    check_json(value, max_bytes=131072)
    if next(Draft202012Validator(schema).iter_errors(value), None) is not None:
        raise WorkflowError("port_validation", "端口值不符合 JSON Schema", node_id=node_id)


def compatible(source: dict[str, JsonValue], target: dict[str, JsonValue]) -> bool:
    left, right = source.get("type"), target.get("type")
    if left is None or right is None:
        return True
    left_types = (
        {left}
        if isinstance(left, str)
        else set(left)
        if isinstance(left, list) and all(isinstance(value, str) for value in left)
        else set()
    )
    right_types = (
        {right}
        if isinstance(right, str)
        else set(right)
        if isinstance(right, list) and all(isinstance(value, str) for value in right)
        else set()
    )
    if "number" in right_types:
        right_types.add("integer")
    return left_types <= right_types
