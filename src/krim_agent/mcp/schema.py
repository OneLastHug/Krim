"""保留 MCP inputSchema，填默认值后以同一合约校验参数。"""

from __future__ import annotations

import copy

from jsonschema import Draft202012Validator
from pydantic import GetJsonSchemaHandler, JsonValue, RootModel, TypeAdapter, model_validator
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema

from krim_agent.protocol.models import check_json


def arguments_model(
    name: str, schema: dict[str, JsonValue]
) -> type[RootModel[dict[str, JsonValue]]]:
    copied = copy.deepcopy(schema)
    check_json(copied)
    Draft202012Validator.check_schema(copied)
    pending: list[JsonValue] = [copied]
    while pending:
        value = pending.pop()
        if isinstance(value, dict):
            reference = value.get("$ref")
            if reference is not None and (
                not isinstance(reference, str) or not reference.startswith("#")
            ):
                raise ValueError("MCP inputSchema 仅允许本地引用")
            pending.extend(value.values())
        elif isinstance(value, list):
            pending.extend(value)

    class Arguments(RootModel[dict[str, JsonValue]]):
        @model_validator(mode="before")
        @classmethod
        def schema_validation(cls, value: object) -> dict[str, JsonValue]:
            parsed = TypeAdapter(dict[str, JsonValue]).validate_python(value, strict=True)
            parsed = copy.deepcopy(parsed)
            properties = copied.get("properties", {})
            if isinstance(properties, dict):
                for key, property_schema in properties.items():
                    if (
                        key not in parsed
                        and isinstance(property_schema, dict)
                        and "default" in property_schema
                    ):
                        parsed[key] = copy.deepcopy(property_schema["default"])
            check_json(parsed)
            if next(Draft202012Validator(copied).iter_errors(parsed), None) is not None:
                raise ValueError("MCP 参数不符合远端 inputSchema")
            return parsed

        @classmethod
        def __get_pydantic_json_schema__(
            cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler
        ) -> JsonSchemaValue:
            return copy.deepcopy(copied)

    Arguments.__name__ = name
    return Arguments
