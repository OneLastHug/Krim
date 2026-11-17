"""Provider-neutral discovery over an immutable, already-authorized tool catalog.

Discovery changes the schemas sent on subsequent model requests, never the full
executor registry or its approval policy. Persist ``snapshot`` with a run before
parking and ``restore`` it after reconstructing the same authorized registry.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Any, cast

from pydantic import BaseModel, Field, JsonValue, field_validator

from krim_agent.context.serialization import fingerprint
from krim_agent.contracts import ServiceError, StrictModel
from krim_agent.source_fingerprints import source_digest

from .core import Decision, Invocation, ToolSpec
from .session import SessionScopeResolver, SessionToolScope, ToolBundle


def scope_key(scope: SessionToolScope) -> tuple[str, ...]:
    """Include run/turn and owner, including when session ids happen to collide."""
    scope.check()
    scope.actor.member(scope.workspace_id)
    return (
        scope.actor.principal_id,
        scope.workspace_id,
        scope.session_id,
        scope.turn_id,
        scope.run_id,
        scope.project_id or "",
    )


class ToolSearchInput(StrictModel):
    query: str = ""
    names: tuple[str, ...] = ()
    handles: tuple[str, ...] = ()
    cursor: str | None = None
    limit: int = Field(default=20, ge=1, le=100)
    activate: bool = True

    @field_validator("names", "handles", mode="before")
    @classmethod
    def json_arrays(cls, value: Any) -> Any:
        # Executor validates an already-decoded JSON object in strict mode.
        return tuple(value) if isinstance(value, list) else value


async def _unbound(arguments: BaseModel) -> JsonValue:
    raise ServiceError("workspace_contract_required")


class ToolCatalogService:
    def __init__(
        self,
        specs: Iterable[ToolSpec],
        *,
        initial_names: Iterable[str] | None = None,
        owner_scope: SessionToolScope | None = None,
    ) -> None:
        frozen = tuple(specs)
        self._specs = {spec.name: spec for spec in frozen}
        if len(self._specs) != len(frozen):
            raise ValueError("tool names conflict")
        self._hashes = {name: spec.fingerprint for name, spec in self._specs.items()}
        self._catalog_hash = fingerprint(cast(Any, self._hashes))
        self._initial = frozenset(
            initial_names
            if initial_names is not None
            else (spec.name for spec in frozen if spec.source is None)
        )
        if self._initial - self._specs.keys():
            raise ServiceError("unknown_tool")
        self._active: dict[tuple[str, ...], set[str]] = {}
        self._owner = scope_key(owner_scope) if owner_scope is not None else None

    def _check(self, scope: SessionToolScope) -> tuple[str, ...]:
        key = scope_key(scope)
        if self._owner is not None and self._owner != key:
            raise ServiceError("permission_denied", 403)
        try:
            if any(spec.fingerprint != self._hashes[name] for name, spec in self._specs.items()):
                raise ValueError("catalog changed")
        except (ValueError, TypeError, RecursionError):
            raise ServiceError("tool_binding_changed") from None
        return key

    def _handle(self, name: str) -> str:
        return name + ":" + self._hashes[name]

    def _names(self, handles: Iterable[str]) -> set[str]:
        names: set[str] = set()
        for handle in handles:
            name, separator, digest = handle.partition(":")
            if not separator or self._hashes.get(name) != digest:
                raise ServiceError("tool_binding_changed")
            names.add(name)
        return names

    def activate(self, scope: SessionToolScope, handles: Iterable[str]) -> tuple[ToolSpec, ...]:
        key = self._check(scope)
        names = self._names(handles)
        self._active.setdefault(key, set(self._initial)).update(names)
        return self.active_specs(scope)

    def active_specs(self, scope: SessionToolScope) -> tuple[ToolSpec, ...]:
        key = self._check(scope)
        active = self._active.get(key, self._initial)
        return tuple(spec for name, spec in self._specs.items() if name in active)

    def snapshot(self, scope: SessionToolScope) -> dict[str, JsonValue]:
        key = self._check(scope)
        return {
            "version": "tool-catalog/1",
            "scope_fingerprint": fingerprint(list(key)),
            "catalog_fingerprint": self._catalog_hash,
            "active_handles": [self._handle(spec.name) for spec in self.active_specs(scope)],
        }

    def restore(self, scope: SessionToolScope, snapshot: Mapping[str, Any]) -> None:
        key = self._check(scope)
        handles = snapshot.get("active_handles")
        if (
            snapshot.get("version") != "tool-catalog/1"
            or snapshot.get("scope_fingerprint") != fingerprint(list(key))
            or snapshot.get("catalog_fingerprint") != self._catalog_hash
            or not isinstance(handles, list)
            or any(not isinstance(handle, str) for handle in handles)
        ):
            raise ServiceError("tool_binding_changed")
        names = self._names(handles)
        self._active[key] = set(self._initial) | names

    def release(self, scope: SessionToolScope) -> None:
        self._active.pop(scope_key(scope), None)

    def search(self, scope: SessionToolScope, body: ToolSearchInput) -> dict[str, JsonValue]:
        self._check(scope)
        if set(body.names) - self._specs.keys():
            raise ServiceError("unknown_tool")
        requested = set(body.names) | self._names(body.handles)
        terms = tuple(re.findall(r"\w+", body.query.casefold()))
        matches: list[ToolSpec] = []
        for spec in self._specs.values():
            if requested and spec.name not in requested:
                continue
            source = spec.source
            haystack = " ".join(
                (
                    spec.name,
                    spec.description,
                    source.server + " " + source.remote_name if source else "",
                )
            ).casefold()
            if all(term in haystack for term in terms):
                matches.append(spec)
        matches.sort(key=lambda spec: (spec.name != body.query.casefold(), spec.name))
        search_hash = fingerprint(
            {
                "catalog": self._catalog_hash,
                "scope": list(scope_key(scope)),
                "query": body.query,
                "names": sorted(requested),
                "limit": body.limit,
            }
        )
        offset = 0
        if body.cursor is not None:
            prefix, separator, position = body.cursor.partition(":")
            if prefix != search_hash or not separator or not position.isdecimal():
                raise ServiceError("invalid_cursor", 422)
            offset = int(position)
            if offset > len(matches):
                raise ServiceError("invalid_cursor", 422)
        page = matches[offset : offset + body.limit]
        if body.activate:
            self.activate(scope, (self._handle(spec.name) for spec in page))
        results: list[JsonValue] = []
        for spec in page:
            entry: dict[str, JsonValue] = {
                "name": spec.name,
                "description": spec.description,
                "handle": self._handle(spec.name),
                "schema_fingerprint": self._hashes[spec.name],
                "risk": spec.risk,
                "source": spec.source.descriptor() if spec.source is not None else None,
            }
            if body.activate:
                entry["input_schema"] = spec.parameters.model_json_schema()
            results.append(entry)
        next_offset = offset + len(page)
        return {
            "catalog_fingerprint": self._catalog_hash,
            "tools": results,
            "next_cursor": f"{search_hash}:{next_offset}" if next_offset < len(matches) else None,
            "total": len(matches),
            "activated": body.activate,
        }

    def bundle(self, resolve: SessionScopeResolver) -> ToolBundle:
        async def execute(
            invocation: Invocation, arguments: BaseModel, context: object
        ) -> JsonValue:
            scope = await resolve(invocation)
            return self.search(scope, cast(ToolSearchInput, arguments))

        async def policy(invocation: Invocation) -> Decision:
            self._check(await resolve(invocation))
            return "allow" if invocation.name == "tool_search" else "deny"

        return ToolBundle(
            (
                ToolSpec(
                    "tool_search",
                    ToolSearchInput,
                    _unbound,
                    {"type": "object"},
                    "tool-catalog/1",
                    fingerprint({
                        "implementation": source_digest(__file__),
                        "catalog": self._catalog_hash,
                        "owner": list(self._owner) if self._owner is not None else None,
                    }),
                    capability="session_read",
                    scoped_executor=execute,
                    description=(
                        "搜索当前会话已授权的工具目录；按名称或关键词分页查找。"
                        "activate=true 返回输入schema并为后续请求启用这些工具。"
                        "句柄绑定当前schema，发现工具不会绕过其审批。"
                    ),
                ),
            ),
            policy,
        )
