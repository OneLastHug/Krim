"""Bounded model-facing argument diagnostics without inputs or validator messages."""

from __future__ import annotations

import math
from typing import Any

from pydantic import JsonValue, ValidationError

from krim_agent.contracts import ServiceError

_MESSAGES = {
    "missing": "Required field is missing.",
    "extra_forbidden": "Unexpected field; use only fields declared by the tool.",
    "less_than_equal": "Value exceeds the maximum.",
    "less_than": "Value must be below the limit.",
    "greater_than_equal": "Value is below the minimum.",
    "greater_than": "Value must be above the limit.",
    "string_too_short": "String is shorter than the minimum length.",
    "string_too_long": "String exceeds the maximum length.",
    "too_short": "Collection is shorter than the minimum length.",
    "too_long": "Collection exceeds the maximum length.",
    "string_pattern_mismatch": "String must match the pattern in the tool schema.",
    "literal_error": "Value must be one of the choices in the tool schema.",
    "enum": "Value must be one of the choices in the tool schema.",
    "int_type": "Expected an integer.",
    "float_type": "Expected a number.",
    "bool_type": "Expected a boolean.",
    "string_type": "Expected a string.",
    "list_type": "Expected an array.",
    "dict_type": "Expected an object.",
    "model_type": "Expected an object matching the tool schema.",
    "finite_number": "Expected a finite number.",
    "multiple_of": "Value must be a multiple of the declared constraint.",
}
_CONSTRAINTS = {
    "less_than_equal": "le", "less_than": "lt",
    "greater_than_equal": "ge", "greater_than": "gt",
    "string_too_short": "min_length", "string_too_long": "max_length",
    "too_short": "min_length", "too_long": "max_length", "multiple_of": "multiple_of",
}


def _field_names(schema: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    pending: list[Any] = [schema]
    while pending:
        node = pending.pop()
        if isinstance(node, dict):
            properties = node.get("properties")
            if isinstance(properties, dict):
                names.update(name for name in properties if isinstance(name, str))
            pending.extend(node.values())
        elif isinstance(node, list):
            pending.extend(node)
    return names


class ToolInputError(ServiceError):
    """Only strict argument validation raises this recoverable pre-dispatch error."""

    def __init__(self, error: ValidationError, schema: dict[str, Any]) -> None:
        super().__init__("invalid_input")
        names = _field_names(schema)
        issues: list[JsonValue] = []
        # Never return error messages, input values, arbitrary extra-field names,
        # custom validator contexts, or schema URLs to the model/logs.
        for item in error.errors(include_url=False, include_input=False, include_context=True)[:8]:
            kind = item["type"] if item["type"] in _MESSAGES else "invalid_value"
            location: list[JsonValue] = [
                part if isinstance(part, str) and part in names
                else part if type(part) is int and 0 <= part <= 1_000_000
                else "<field>"
                for part in item["loc"][:8]
            ]
            issue: dict[str, JsonValue] = {
                "field": location, "type": kind,
                "message": _MESSAGES.get(kind, "Value does not satisfy the tool schema."),
            }
            bound = _CONSTRAINTS.get(kind)
            value = (item.get("ctx") or {}).get(bound) if bound else None
            if type(value) in (int, float) and abs(value) <= 10**18 and math.isfinite(value):
                issue["constraint"] = {bound: value}
            issues.append(issue)
        self.details: dict[str, JsonValue] = {
            "code": "invalid_input", "executor_started": False,
            "retryable": True, "errors": issues,
            "truncated": error.error_count() > len(issues),
            "message": ("No calls in this batch executed. Correct the arguments using the "
                        "tool schema and submit the batch again."),
        }


def blocked_batch_result() -> dict[str, JsonValue]:
    return {
        "code": "batch_not_executed", "executor_started": False,
        "retryable": True,
        "message": ("No calls in this batch executed because another call has invalid "
                    "arguments. Correct that call before submitting this batch again."),
    }
