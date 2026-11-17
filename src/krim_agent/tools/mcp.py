"""把已发现的 MCP 工具适配为本分支静态 ToolSpec。"""

from __future__ import annotations

import inspect
import json
import re
from collections import Counter
from copy import deepcopy
from typing import Any, cast

from pydantic import BaseModel, ConfigDict, JsonValue, RootModel, model_validator

from krim_agent.contracts import ServiceError, check_json, fingerprint
from krim_agent.mcp import MCPConnection
from krim_agent.source_fingerprints import source_digest

from .core import ExternalExecutor, Invocation, ToolSource, ToolSpec
from .mcp_schema import MCPContract


def arguments_model(name: str, schema: dict[str, JsonValue]) -> type[BaseModel]:
    """完整复用原 JSON Schema，仅转换 BaseModel 的调用接口。"""
    if schema.get("type") != "object":
        raise ValueError("MCP 工具输入须为对象")
    contract = MCPContract(schema)
    schema_json = json.dumps(contract.schema(), sort_keys=True)

    class MCPArguments(RootModel[dict[str, JsonValue]]):
        model_config = ConfigDict(strict=True, frozen=True, hide_input_in_errors=True)

        @model_validator(mode="before")
        @classmethod
        def validate_arguments(cls, value: Any) -> Any:
            try:
                check_json(value)
                contract.validate(value)
            except (ValueError, TypeError, RecursionError):
                raise ValueError("MCP 参数不符合来源 Schema") from None
            return value

        @classmethod
        def model_json_schema(cls, *args: Any, **kwargs: Any) -> dict[str, Any]:
            return cast(dict[str, Any], json.loads(schema_json))

    MCPArguments.__name__ = name
    return MCPArguments


async def _without_invocation(arguments: BaseModel) -> JsonValue:
    raise ServiceError("tool_binding_changed")


def mcp_tools(
    connection: MCPConnection, *, namespace: str | None = None,
) -> tuple[ToolSpec, ...]:
    config = connection.config
    if namespace is not None and re.fullmatch(r"[a-z][a-z0-9_]*", namespace) is None:
        raise ValueError("invalid MCP tool namespace")

    def normalized(remote: str) -> str:
        prefix = namespace + "__" if namespace is not None else ""
        return re.sub(r"[^a-z0-9_]", "_", (prefix + config.name + "__" + remote).lower())

    selected = set(config.selected_tools)
    if any(len(remote) > 128 or any(ord(char) < 32 for char in remote) for remote in selected):
        raise ServiceError("invalid_input", 422)
    normalized_names = Counter(normalized(remote) for remote in selected)
    public_names = {remote: normalized(remote) for remote in selected}
    needs_hash = {remote for remote, name in public_names.items()
                  if len(name) > 64 or normalized_names[name] > 1}
    used = {name for remote, name in public_names.items() if remote not in needs_hash}
    for remote in sorted(needs_hash):
        base = public_names[remote]
        material = {"namespace": namespace, "server": config.name, "tool": remote}
        candidate = base[:47] + "_" + fingerprint(material)[:16]
        nonce = 0
        while candidate in used:
            nonce += 1
            candidate = base[:47] + "_" + fingerprint({**material, "collision": nonce})[:16]
        public_names[remote] = candidate
        used.add(candidate)
    available = {item.name: item for item in connection.tools}
    if selected - set(available):
        raise ServiceError("unknown_tool")
    result: list[ToolSpec] = []
    for remote in config.selected_tools:
        item = available[remote]
        name = public_names[remote]
        if re.fullmatch(r"[a-z][a-z0-9_]{0,63}", name) is None:
            raise ServiceError("invalid_input", 422)
        parameters = arguments_model("MCP_" + name, deepcopy(item.input_schema))
        output = item.output_schema if item.output_schema is not None else True
        if not isinstance(output, (dict, bool)):
            raise ServiceError("invalid_input", 422)
        source = ToolSource("mcp", config.name, remote, config.configuration_digest)
        invoke = _executor(connection, remote)
        result.append(
            ToolSpec(
                name,
                parameters,
                _without_invocation,
                output,
                "mcp/" + config.version,
                fingerprint(
                    {
                        "adapter": source_digest(__file__),
                        "source": source.descriptor(),
                        "transport": source_digest(inspect.getfile(type(connection))),
                        "schema_validator": source_digest(inspect.getfile(MCPContract)),
                    }
                ),
                risk="read" if item.read_only else "write",
                description=item.description or "",
                source=source,
                external_executor=invoke,
            )
        )
    return tuple(result)


def _executor(connection: MCPConnection, remote: str) -> ExternalExecutor:
    async def execute(invocation: Invocation, arguments: BaseModel) -> JsonValue:
        if connection.closed:
            raise ServiceError("mcp_unavailable", 503)
        return await connection.call_tool(remote, arguments.model_dump(mode="json"))

    return execute
