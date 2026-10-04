"""工具的 JSON 输出合约；只解析本地 Schema，不检索远端资源。"""

from __future__ import annotations

import copy
from collections.abc import Iterator

from jsonschema import Draft202012Validator, FormatChecker, SchemaError
from pydantic import BaseModel, JsonValue, TypeAdapter
from referencing import Registry
from referencing.exceptions import Unresolvable

from krim_agent.protocol.models import check_json

OutputSchema = type[BaseModel] | dict[str, JsonValue] | bool
WireOutputSchema = dict[str, JsonValue] | bool


def _schema_nodes(schema: WireOutputSchema) -> Iterator[dict[str, JsonValue]]:
    pending: list[JsonValue] = [schema]
    mappings = {"$defs", "definitions", "properties", "patternProperties", "dependentSchemas"}
    singles = {
        "items",
        "additionalItems",
        "contains",
        "unevaluatedItems",
        "additionalProperties",
        "unevaluatedProperties",
        "propertyNames",
        "not",
        "if",
        "then",
        "else",
        "contentSchema",
    }
    arrays = {"allOf", "anyOf", "oneOf", "prefixItems"}
    while pending:
        current = pending.pop()
        if not isinstance(current, dict):
            continue
        yield current
        for key, value in current.items():
            if key in mappings and isinstance(value, dict):
                pending.extend(value.values())
            elif key in singles:
                pending.append(value)
            elif key in arrays and isinstance(value, list):
                pending.extend(value)


class OutputContract:
    def __init__(self, declaration: OutputSchema) -> None:
        schema = (
            declaration.model_json_schema(mode="serialization")
            if isinstance(declaration, type) and issubclass(declaration, BaseModel)
            else declaration
        )
        self._schema: WireOutputSchema = copy.deepcopy(
            TypeAdapter(WireOutputSchema).validate_python(schema, strict=True)
        )
        check_json(self._schema)
        if isinstance(self._schema, dict):
            dialect: JsonValue = self._schema.get("$schema")
            if dialect is not None and (
                not isinstance(dialect, str)
                or dialect.rstrip("#") != "https://json-schema.org/draft/2020-12/schema"
            ):
                raise ValueError("工具输出 Schema 只支持 Draft 2020-12")
        for node in _schema_nodes(self._schema):
            for key in ("$ref", "$dynamicRef", "$recursiveRef"):
                reference = node.get(key)
                if reference is not None and (
                    not isinstance(reference, str) or not reference.startswith("#")
                ):
                    raise ValueError("工具输出 Schema 只允许本地引用")
        try:
            Draft202012Validator.check_schema(self._schema)
        except SchemaError as error:
            raise ValueError("工具输出 Schema 不合法") from error

    def schema(self) -> WireOutputSchema:
        return copy.deepcopy(self._schema)

    def validate(self, value: JsonValue) -> None:
        try:
            validator = Draft202012Validator(
                self._schema, format_checker=FormatChecker(), registry=Registry()
            )
            if next(validator.iter_errors(value), None) is not None:
                raise ValueError("工具结果不符合声明的输出 Schema")
        except (Unresolvable, RecursionError) as error:
            raise ValueError("工具结果的输出 Schema 无法在本地验证") from error
