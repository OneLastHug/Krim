"""MCP Schema 使用已有 Rust 正则引擎，避免远端正则阻塞事件循环。"""

from collections.abc import Callable, Iterator
from typing import Any, cast

from jsonschema import Draft202012Validator, FormatChecker, ValidationError, validators
from jsonschema.protocols import Validator
from pydantic import JsonValue
from pydantic_core import SchemaError, SchemaValidator, core_schema
from pydantic_core import ValidationError as PatternError
from referencing import Registry
from referencing.exceptions import Unresolvable

from .output import OutputContract, OutputSchema, _schema_nodes


class MCPContract(OutputContract):
    def __init__(self, declaration: OutputSchema) -> None:
        super().__init__(declaration)
        validation_schema = self.schema()
        patterns: dict[str, SchemaValidator] = {}

        def compiled(pattern: str) -> SchemaValidator:
            if pattern not in patterns:
                if len(pattern) > 2048:
                    raise ValueError("MCP正则约束超限")
                try:
                    patterns[pattern] = SchemaValidator(
                        core_schema.str_schema(
                            pattern=pattern, strict=True, regex_engine="rust-regex"
                        )
                    )
                except SchemaError:
                    raise ValueError("MCP正则约束不受支持") from None
            return patterns[pattern]

        for node in _schema_nodes(validation_schema):
            # jsonschema 对该关键字的内部属性归属算法仍使用 Python re。
            if "patternProperties" in node:
                raise ValueError("MCP Schema暂不支持patternProperties")
            dialect = node.pop("$schema", None)
            if dialect is not None and (
                not isinstance(dialect, str)
                or dialect.rstrip("#") != "https://json-schema.org/draft/2020-12/schema"
            ):
                raise ValueError("MCP Schema只支持Draft 2020-12")
            pattern = node.get("pattern")
            if isinstance(pattern, str):
                compiled(pattern)

        def pattern_validator(
            validator: Any, rule: str, instance: Any, schema: Any
        ) -> Iterator[ValidationError]:
            if isinstance(instance, str):
                try:
                    compiled(rule).validate_python(instance)
                except PatternError:
                    yield ValidationError("字符串不符合MCP正则约束")

        checker = FormatChecker()

        @checker.checks("regex")
        def regex_format(value: Any) -> bool:
            if not isinstance(value, str):
                return True
            if len(value) > 2048:
                return False
            try:
                # 用户提供的regex只做语法检查，不积累到静态Schema缓存。
                SchemaValidator(core_schema.str_schema(pattern=value, regex_engine="rust-regex"))
                return True
            except SchemaError:
                return False

        extend = cast(Callable[..., Callable[..., Validator]], validators.extend)
        safe_type = extend(Draft202012Validator, {"pattern": pattern_validator})
        self._validator = safe_type(validation_schema, format_checker=checker, registry=Registry())

    def validate(self, value: JsonValue) -> None:
        try:
            if next(self._validator.iter_errors(value), None) is not None:
                raise ValueError("MCP数据不符合来源Schema")
        except (Unresolvable, RecursionError) as error:
            raise ValueError("MCP Schema无法在本地验证") from error
