"""Session-authorized resource access over the selected MCP connections only."""

from __future__ import annotations

import copy
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any, cast

from pydantic import BaseModel, Field, JsonValue, model_validator

from krim_agent.context.serialization import fingerprint
from krim_agent.contracts import ServiceError, StrictModel
from krim_agent.mcp import MCPConnection
from krim_agent.source_fingerprints import source_digest

from .catalog_search import scope_key
from .core import Decision, Invocation, ToolSpec
from .session import SessionScopeResolver, SessionToolScope, ToolBundle

# The optional media consumer receives the original contents and can register
# authorized media references. This module never claims base64 is a model image.
type ResourceMediaSink = Callable[
    [SessionToolScope, str, dict[str, JsonValue]], Awaitable[JsonValue]
]


@dataclass(frozen=True)
class MCPResourceBinding:
    source_id: str
    connection: MCPConnection
    owner_scope: SessionToolScope | None = None


class ListResourcesInput(StrictModel):
    server: str | None = None
    cursor: str | None = None


class ReadResourceInput(StrictModel):
    server: str
    uri: str | None = None
    result_ref: str | None = None
    offset: int = Field(default=0, ge=0)
    max_chars: int = Field(default=12000, ge=1, le=48000)

    @model_validator(mode="after")
    def reference_or_uri(self) -> ReadResourceInput:
        if (self.uri is None) == (self.result_ref is None):
            raise ValueError("provide uri or result_ref")
        if self.uri is not None and not self.uri:
            raise ValueError("resource uri is empty")
        return self


async def _unbound(arguments: BaseModel) -> JsonValue:
    raise ServiceError("workspace_contract_required")


class _ResourceNotFound(ServiceError):
    """A local lookup missed; not an authorization or remote execution failure."""

    def __init__(self) -> None:
        super().__init__("not_found", 404)


class MCPResourceService:
    def __init__(
        self,
        connections: Iterable[MCPResourceBinding],
        *,
        media_sink: ResourceMediaSink | None = None,
    ) -> None:
        bindings = tuple(connections)
        self._bindings = {item.source_id: item.connection for item in bindings}
        if any(not item.source_id for item in bindings) or len(self._bindings) != len(bindings):
            raise ValueError("MCP resource source ids must be unique")
        self._owners = {
            item.source_id: scope_key(item.owner_scope) if item.owner_scope is not None else None
            for item in bindings
        }
        self._digests = {
            name: connection.config.configuration_digest
            for name, connection in self._bindings.items()
        }
        self._media_sink = media_sink
        self._results: dict[tuple[tuple[str, ...], str], dict[str, JsonValue]] = {}
        self._cursors: dict[str, tuple[tuple[str, ...], str, bool, str, tuple[str, ...]]] = {}

    def _connection(self, scope: SessionToolScope, source_id: str) -> MCPConnection:
        key = scope_key(scope)
        connection = self._bindings.get(source_id)
        if connection is None or self._owners[source_id] not in (None, key):
            raise _ResourceNotFound()
        if connection.closed:
            raise ServiceError("mcp_connection_closed", 503)
        if connection.config.configuration_digest != self._digests[source_id]:
            raise ServiceError("tool_binding_changed")
        return connection

    def sources(self, scope: SessionToolScope) -> list[JsonValue]:
        key = scope_key(scope)
        return [
            {
                "server": name,
                "name": connection.config.name,
                "configuration_digest": self._digests[name],
            }
            for name, connection in self._bindings.items()
            if self._owners[name] in (None, key)
            and not connection.closed
            and getattr(connection, "supports_resources", True)
        ]

    async def list(
        self,
        scope: SessionToolScope,
        body: ListResourcesInput,
        *,
        templates: bool = False,
    ) -> dict[str, JsonValue]:
        key = scope_key(scope)
        field = "resourceTemplates" if templates else "resources"
        if body.server is None:
            if body.cursor is not None:
                raise ServiceError("invalid_cursor", 422)
            return {"sources": self.sources(scope), field: [], "next_cursor": None}
        connection = self._connection(scope, body.server)
        cursor = None
        ancestry: tuple[str, ...] = ()
        if body.cursor is not None:
            known = self._cursors.get(body.cursor)
            if known is None or known[:3] != (key, body.server, templates):
                raise ServiceError("invalid_cursor", 422)
            cursor = known[3]
            ancestry = known[4] + (cursor,)
        page = await (
            connection.list_resource_templates(cursor)
            if templates
            else connection.list_resources(cursor)
        )
        self._connection(scope, body.server)
        if not isinstance(page, dict) or not isinstance(page.get(field), list):
            raise ServiceError("mcp_invalid_json", 502)
        remote_next = page.get("nextCursor")
        next_cursor = None
        if remote_next is not None:
            if not isinstance(remote_next, str) or not remote_next or remote_next in ancestry:
                raise ServiceError("mcp_cursor_invalid", 502)
            next_cursor = fingerprint(
                {
                    "scope": list(key),
                    "server": body.server,
                    "configuration": self._digests[body.server],
                    "templates": templates,
                    "cursor": remote_next,
                }
            )
            self._cursors[next_cursor] = (key, body.server, templates, remote_next, ancestry)
        return {
            "server": body.server,
            "configuration_digest": self._digests[body.server],
            field: copy.deepcopy(page[field]),
            "next_cursor": next_cursor,
        }

    async def read(self, scope: SessionToolScope, body: ReadResourceInput) -> dict[str, JsonValue]:
        key = scope_key(scope)
        connection = self._connection(scope, body.server)
        if body.result_ref is None:
            assert body.uri is not None
            result = await connection.read_resource(body.uri)
            self._connection(scope, body.server)
            if not isinstance(result, dict) or not isinstance(result.get("contents"), list):
                raise ServiceError("mcp_invalid_json", 502)
            record: dict[str, JsonValue] = {
                "server": body.server,
                "uri": body.uri,
                "configuration_digest": self._digests[body.server],
                "contents": copy.deepcopy(result["contents"]),
            }
            result_ref = fingerprint(record)
            self._results[key, result_ref] = record
        else:
            result_ref = body.result_ref
            record = self.result(scope, result_ref)
            if record["server"] != body.server:
                raise _ResourceNotFound()
        remaining = body.max_chars
        skip = body.offset
        total = 0
        output: list[JsonValue] = []
        for raw in cast(list[Any], record["contents"]):
            if not isinstance(raw, dict):
                raise ServiceError("mcp_invalid_json", 502)
            item = dict(raw)
            field = "text" if "text" in item else "blob" if "blob" in item else None
            if field is None or not isinstance(item[field], str):
                raise ServiceError("mcp_invalid_json", 502)
            content = item.pop(field)
            total += len(content)
            start = min(skip, len(content))
            skip -= start
            chunk = content[start : start + remaining]
            remaining -= len(chunk)
            item.update({field: chunk, "offset": start, "total_chars": len(content)})
            # Empty pages retain MIME/URI and length metadata; binary is tagged
            # as base64, never mislabeled as a rendered model image.
            if field == "blob":
                item["encoding"] = "base64"
            output.append(cast(JsonValue, item))
        next_offset = body.offset + body.max_chars
        return {
            "server": body.server,
            "uri": record["uri"],
            "result_ref": result_ref,
            "configuration_digest": record["configuration_digest"],
            "contents": output,
            "total_chars": total,
            "next_offset": next_offset if next_offset < total else None,
        }

    def result(self, scope: SessionToolScope, result_ref: str) -> dict[str, JsonValue]:
        """Full content for an authorized artifact/media consumer, never a host URI fetch."""
        record = self._results.get((scope_key(scope), result_ref))
        if record is None:
            raise _ResourceNotFound()
        self._connection(scope, cast(str, record["server"]))
        return copy.deepcopy(record)

    def snapshot(self, scope: SessionToolScope) -> dict[str, JsonValue]:
        """Host persistence port: keep full reads so resume never replays a request."""
        key = scope_key(scope)
        records = {
            ref: copy.deepcopy(record)
            for (owner, ref), record in self._results.items() if owner == key
        }
        cursors = {
            token: [value[1], value[2], value[3], list(value[4])]
            for token, value in self._cursors.items() if value[0] == key
        }
        return cast(dict[str, JsonValue], {
            "version": "mcp-resources/1", "scope_fingerprint": fingerprint(list(key)),
            "bindings": dict(self._digests), "results": records, "cursors": cursors,
        })

    def restore(self, scope: SessionToolScope, snapshot: dict[str, Any]) -> None:
        key = scope_key(scope)
        if (
            snapshot.get("version") != "mcp-resources/1"
            or snapshot.get("scope_fingerprint") != fingerprint(list(key))
            or snapshot.get("bindings") != self._digests
            or not isinstance(snapshot.get("results"), dict)
            or not isinstance(snapshot.get("cursors"), dict)
        ):
            raise ServiceError("tool_binding_changed")
        results: dict[tuple[tuple[str, ...], str], dict[str, JsonValue]] = {}
        cursors: dict[str, tuple[tuple[str, ...], str, bool, str, tuple[str, ...]]] = {}
        for ref, record in snapshot["results"].items():
            if not isinstance(record, dict) or fingerprint(record) != ref:
                raise ServiceError("facts_corrupt")
            server = record.get("server")
            if not isinstance(server, str):
                raise ServiceError("facts_corrupt")
            self._connection(scope, server)
            if record.get("configuration_digest") != self._digests[server]:
                raise ServiceError("tool_binding_changed")
            results[key, ref] = copy.deepcopy(record)
        for token, value in snapshot["cursors"].items():
            if (
                not isinstance(token, str) or not isinstance(value, list) or len(value) != 4
                or not isinstance(value[0], str) or type(value[1]) is not bool
                or not isinstance(value[2], str) or not isinstance(value[3], list)
                or any(not isinstance(item, str) for item in value[3])
            ):
                raise ServiceError("facts_corrupt")
            server, templates, remote, ancestry = value
            self._connection(scope, server)
            expected = fingerprint({
                "scope": list(key), "server": server,
                "configuration": self._digests[server],
                "templates": templates, "cursor": remote,
            })
            if token != expected:
                raise ServiceError("facts_corrupt")
            cursors[token] = (key, server, templates, remote, tuple(ancestry))
        self._results.update(results)
        self._cursors.update(cursors)

    def release(self, scope: SessionToolScope) -> None:
        key = scope_key(scope)
        for entry in tuple(self._results):
            if entry[0] == key:
                del self._results[entry]
        for token, value in tuple(self._cursors.items()):
            if value[0] == key:
                del self._cursors[token]

    def bundle(self, resolve: SessionScopeResolver) -> ToolBundle:
        def missing(scope: SessionToolScope) -> dict[str, JsonValue]:
            # Keep unknown and invisible sources indistinguishable. Only the
            # tool boundary recovers; storage/media consumers retain the 404.
            scope_key(scope)
            return {
                "ok": False,
                "code": "not_found",
                "next_action": (
                    "Call list_mcp_resources without server to discover available sources. "
                    "Use an exact returned server identifier, then list its resources or "
                    "templates before reading a URI. A result_ref is valid only in the "
                    "current run and for its original server. If no sources are available, "
                    "continue the task without MCP resources; do not invent server names."
                ),
            }

        async def listing(
            invocation: Invocation, arguments: BaseModel, context: object
        ) -> JsonValue:
            scope = await resolve(invocation)
            try:
                return await self.list(scope, cast(ListResourcesInput, arguments))
            except _ResourceNotFound:
                return missing(scope)

        async def templates(
            invocation: Invocation, arguments: BaseModel, context: object
        ) -> JsonValue:
            scope = await resolve(invocation)
            try:
                return await self.list(scope, cast(ListResourcesInput, arguments), templates=True)
            except _ResourceNotFound:
                return missing(scope)

        async def read(invocation: Invocation, arguments: BaseModel, context: object) -> JsonValue:
            scope = await resolve(invocation)
            values = cast(ReadResourceInput, arguments)
            try:
                result = await self.read(scope, values)
            except _ResourceNotFound:
                return missing(scope)
            if self._media_sink is not None and values.result_ref is None:
                record = self.result(scope, cast(str, result["result_ref"]))
                result["media"] = await self._media_sink(scope, values.server, record)
                scope.check()
            return result

        binding_digest = fingerprint({
            "implementation": source_digest(__file__),
            "bindings": self._digests,
            "owners": {name: list(owner) if owner is not None else None
                       for name, owner in self._owners.items()},
        })
        specs = tuple(
            ToolSpec(
                name,
                parameters,
                _unbound,
                {"type": "object"},
                "mcp-resources/1",
                binding_digest,
                capability="session_read",
                scoped_executor=execute,
                description=description,
            )
            for name, parameters, execute, description in (
                (
                    "list_mcp_resources",
                    ListResourcesInput,
                    listing,
                    "列出所选MCP连接的资源；省略server先查看可用来源，使用next_cursor分页。",
                ),
                (
                    "list_mcp_resource_templates",
                    ListResourcesInput,
                    templates,
                    "列出所选MCP连接的URI模板；省略server查看来源，使用next_cursor分页。",
                ),
                (
                    "read_mcp_resource",
                    ReadResourceInput,
                    read,
                    "通过所选MCP连接读取URI。大结果返回result_ref，以offset继续读取已保存内容；"
                    "blob为base64原始数据。URI不会在宿主直接打开。",
                ),
            )
        )
        names = {spec.name for spec in specs}

        async def policy(invocation: Invocation) -> Decision:
            scope_key(await resolve(invocation))
            return "allow" if invocation.name in names else "deny"

        return ToolBundle(specs, policy)
