"""Shared bounded completion validation for workspace and runtime consumers."""

from __future__ import annotations

import ast
import re
from typing import Literal

from pydantic import Field, JsonValue, field_validator

from krim_agent.contracts import StrictModel, check_json, encoded, parse_json
from krim_agent.validation.files import path_parts


class CompletionCondition(StrictModel):
    id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    kind: Literal["json_equal", "ast_symbols"]
    path: str
    pointer: str = ""
    expected: JsonValue = None
    symbols: tuple[str, ...] = Field(default=(), max_length=16)

    @field_validator("path")
    @classmethod
    def path_valid(cls, value: str) -> str:
        path_parts(value)
        return value

    @field_validator("expected")
    @classmethod
    def expected_valid(cls, value: JsonValue) -> JsonValue:
        check_json(value, max_bytes=4096)
        return value


def condition(
    kind: str,
    content: bytes,
    *,
    pointer: str,
    expected: JsonValue,
    symbols: tuple[str, ...],
) -> bool:
    """Evaluate one bounded JSON equality or top-level AST symbol condition."""
    if kind == "json_equal":
        value = parse_json(content.decode("utf-8"))
        if len(pointer.encode()) > 256 or (pointer and not pointer.startswith("/")):
            raise ValueError("pointer无效")
        parts = pointer.split("/")[1:] if pointer else []
        if len(parts) > 16:
            raise ValueError("pointer过深")
        for part in parts:
            if re.search(r"~(?![01])", part):
                raise ValueError("pointer转义无效")
            key = part.replace("~1", "/").replace("~0", "~")
            if type(value) is dict:
                value = value[key]
            elif type(value) is list and re.fullmatch(r"0|[1-9][0-9]*", key):
                value = value[int(key)]
            else:
                raise ValueError("pointer类型无效")
        # Canonical JSON keeps bool/number and integer/float representations distinct.
        return encoded(value) == encoded(expected)

    tree = ast.parse(content.decode("utf-8"))
    pending: list[tuple[ast.AST, int]] = [(tree, 0)]
    count = 0
    while pending:
        node, depth = pending.pop()
        count += 1
        if count > 50_000 or depth > 128:
            raise ValueError("AST超限")
        pending.extend((child, depth + 1) for child in ast.iter_child_nodes(node))
    names = {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    }
    return set(symbols).issubset(names)
