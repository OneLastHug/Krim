"""One run's authorized tools, model-visible selection, and durable checkpoints.

The helper owns only account connections it opens. Host MCP/Web connections are
borrowed from ToolRuntime and stay alive when a run is parked or closed. Callers
persist checkpoint() alongside SDK state before closing a parked run; restore()
never replays any resource read or external action.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Protocol, cast

from pydantic import JsonValue

from krim_agent.context.serialization import fingerprint
from krim_agent.contracts import ServiceError
from krim_agent.mcp import MCPConnection
from krim_agent.runtime.contracts import settled
from krim_agent.tools.bundles import ModeGuard, compose_executor
from krim_agent.tools.catalog_search import ToolCatalogService, scope_key
from krim_agent.tools.core import ToolSpec
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.mcp_resources import (
    MCPResourceBinding,
    MCPResourceService,
    ResourceMediaSink,
)
from krim_agent.tools.registry import ToolRegistry
from krim_agent.tools.session import SessionScopeResolver, SessionToolScope, ToolBundle

if TYPE_CHECKING:
    from krim_agent.account_mcp.service import AccountMCPBundle, AccountMCPService
    from krim_agent.tools.runtime import ToolRuntime


def trusted_readonly_names(
    specs: Iterable[ToolSpec], connections: Iterable[MCPConnection],
) -> frozenset[str]:
    """Derive planning trust from selected host/owner configuration, not hints.

    A source must match an actual connection's server and configuration digest,
    and its remote name must be both selected and explicitly readonly. Requiring
    read risk additionally honors an explicit remote readOnlyHint=false. Aliases
    are trusted only when their complete binding chain passes the same checks.
    Resource tools have no such configured source and stay outside this set.
    """
    configured = {}
    unavailable = set()
    for connection in connections:
        config = connection.config
        key = (config.name, config.configuration_digest)
        if connection.closed:
            unavailable.add(key)
        else:
            configured[key] = config
    for key in unavailable:
        configured.pop(key, None)
    result: set[str] = set()
    for spec in specs:
        chain: list[ToolSpec] = []
        seen: set[int] = set()
        current: ToolSpec | None = spec
        while current is not None:
            if id(current) in seen:
                raise ServiceError("tool_binding_changed")
            seen.add(id(current))
            source = current.source
            if source is None or source.kind != "mcp" or current.risk != "read":
                break
            matched = configured.get((source.server, source.configuration_digest))
            if (matched is None or source.remote_name not in matched.selected_tools
                    or source.remote_name not in matched.readonly_tools):
                break
            chain.append(current)
            current = current.binding_target
        else:
            result.update(item.name for item in chain)
    return frozenset(result)


class WebResultState(Protocol):
    def snapshot(self, scope: str) -> dict[str, Any]: ...
    def restore(self, scope: str, state: dict[str, Any]) -> None: ...


class RunTools:
    def __init__(
        self, executor: ToolExecutor, scope: SessionToolScope,
        catalog: ToolCatalogService, resources: MCPResourceService,
        *, account: AccountMCPBundle | None = None, web: WebResultState | None = None,
        connections: Iterable[MCPConnection] = (),
    ) -> None:
        self.executor = executor
        self.scope = scope
        self.catalog = catalog
        self.resources = resources
        self.account = account
        self.web = web
        self._connections = tuple(connections)
        self._scope_key = scope_key(scope)
        self._registry_hash = fingerprint(executor.registry.schemas())
        self._close_task: asyncio.Task[None] | None = None

    def _check(self) -> None:
        if self._close_task is not None:
            raise ServiceError("closed", 409)
        if scope_key(self.scope) != self._scope_key:
            raise ServiceError("permission_denied", 403)
        self.executor.registry.assert_current()
        if fingerprint(self.executor.registry.schemas()) != self._registry_hash:
            raise ServiceError("tool_binding_changed")

    @property
    def trusted_readonly_names(self) -> frozenset[str]:
        self._check()
        return trusted_readonly_names(
            (spec for spec, _ in self.executor.registry._frozen), self._connections,
        )

    def schemas(self) -> list[dict[str, JsonValue]]:
        """Current model input; complete executor authority is not reduced."""
        self._check()
        active = {spec.name for spec in self.catalog.active_specs(self.scope)}
        active.add("tool_search")
        return [descriptor for descriptor in self.executor.registry.schemas()
                if descriptor["name"] in active]

    def checkpoint(self) -> dict[str, Any]:
        self._check()
        state: dict[str, Any] = {
            "version": "run-tools/1",
            "scope_fingerprint": fingerprint(list(self._scope_key)),
            "registry_fingerprint": self._registry_hash,
            "catalog": self.catalog.snapshot(self.scope),
            "resources": self.resources.snapshot(self.scope),
        }
        if self.web is not None:
            state["web"] = self.web.snapshot(self.scope.run_id)
        return state

    def restore(self, state: dict[str, Any]) -> None:
        """Restore only the identical authorized run, catalog, and connections."""
        self._check()
        if (
            state.get("version") != "run-tools/1"
            or state.get("scope_fingerprint") != fingerprint(list(self._scope_key))
            or state.get("registry_fingerprint") != self._registry_hash
            or not isinstance(state.get("catalog"), dict)
            or not isinstance(state.get("resources"), dict)
            or self.web is not None and not isinstance(state.get("web"), dict)
            or self.web is None and "web" in state
        ):
            raise ServiceError("tool_binding_changed")
        # Roll back local selection if resource/Web restoration fails. Web may
        # validate local immutable content files, never refetch an external URL.
        previous_catalog = self.catalog.snapshot(self.scope)
        previous_resources = self.resources.snapshot(self.scope)
        previous_web = self.web.snapshot(self.scope.run_id) if self.web is not None else None
        try:
            self.catalog.restore(self.scope, state["catalog"])
            self.resources.release(self.scope)
            self.resources.restore(self.scope, state["resources"])
            if self.web is not None:
                self.web.restore(self.scope.run_id, state["web"])
        except BaseException:
            self.catalog.restore(self.scope, previous_catalog)
            self.resources.release(self.scope)
            self.resources.restore(self.scope, previous_resources)
            if self.web is not None and previous_web is not None:
                self.web.restore(self.scope.run_id, previous_web)
            raise

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await settled(self._close_task)

    async def _close(self) -> None:
        try:
            if self.account is not None:
                await self.account.close()
        finally:
            # Cancellation/revocation must not prevent disposal of our own
            # cached state. No read or external action occurs during release.
            cleanup_scope = replace(self.scope, check=lambda: None)
            self.catalog.release(cleanup_scope)
            self.resources.release(cleanup_scope)


async def open_run_tools(
    base: ToolExecutor,
    *,
    scope: SessionToolScope,
    resolve: SessionScopeResolver,
    host: ToolRuntime | None = None,
    account_mcp: AccountMCPService | None = None,
    bundles: Iterable[ToolBundle] = (),
    guard: ModeGuard | None = None,
    media_sink: ResourceMediaSink | None = None,
) -> RunTools:
    """Compose one executor, preserving base hooks and guarding every tool.

    scope comes from SessionToolHost.by_run(run_id), not a fabricated Invocation.
    Tool execution policies still call resolve(invocation) to reauthorize live
    access. Guard is the host's current plan-mode policy and also covers account
    tools with unknown side effects. The run owner must persist checkpoint data
    outside compacted model text before disposing this object.
    """
    scope_key(scope)
    base.registry.assert_current()
    account: AccountMCPBundle | None = None
    selected = list(bundles)
    bindings: list[MCPResourceBinding] = []
    try:
        if host is not None:
            host_bundle = host.bundle(resolve)
            host_specs = {spec.name: spec for spec in host_bundle.specs}
            retained = []
            for spec, frozen_hash in base.registry._frozen:
                replacement = host_specs.get(spec.name)
                if replacement is None:
                    retained.append(spec)
                elif frozen_hash != replacement.fingerprint:
                    raise ServiceError("tool_binding_changed")
            if len(retained) != len(base.registry._frozen):
                # Early legacy composition already included these exact host
                # tools. Rebind only identical specs to live scope wrappers;
                # keep the base native policy and the complete host pipeline.
                base = ToolExecutor(
                    ToolRegistry(tuple(retained), dependencies=(base.registry,)),
                    base.policy, base.pre, base.post,
                    middleware=base.middleware, wrap=base.wrap,
                )
            selected.append(host_bundle)
            bindings.extend(MCPResourceBinding(
                "host:" + connection.config.name, connection, scope,
            ) for connection in host.connections)
        if account_mcp is not None:
            configs = await account_mcp.configurations_for_turn(scope.turn_id)
            scope.check()
            account = await account_mcp.open_bundle(configs, resolve, scope=scope)
            selected.append(account.bundle)
            bindings.extend(MCPResourceBinding(
                "account:" + connection.config.name, connection, scope,
            ) for connection in account.connections)
        resources = MCPResourceService(bindings, media_sink=media_sink)
        selected.append(resources.bundle(resolve))
        ordinary = compose_executor(base, selected)
        # Source-less native/session tools form the small always-active core;
        # external schemas load via discovery. tool_search itself is appended
        # separately so it can never be hidden by its own selection state.
        ordinary_specs = tuple(spec for spec, _ in ordinary.registry._frozen)
        catalog = ToolCatalogService(ordinary_specs, owner_scope=scope)
        executor = compose_executor(
            ordinary, (catalog.bundle(resolve),), guard=guard,
            guard_confirms=guard is not None and scope.project_id is not None,
        )
        return RunTools(
            executor, scope, catalog, resources, account=account,
            connections=(binding.connection for binding in bindings),
            web=(cast(WebResultState, host.web)
                 if host is not None and host.web is not None else None),
        )
    except BaseException:
        if account is not None:
            await settled(account.close())
        raise
