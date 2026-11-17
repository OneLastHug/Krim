"""B's derived registries keep the original source registry's freeze fence."""

from dataclasses import replace
from types import MappingProxyType

import pytest

from krim_agent.contracts import ServiceError, StrictModel
from krim_agent.runtime.run_tools import open_run_tools
from krim_agent.security import LOCAL
from krim_agent.tools.core import ToolSource, ToolSpec
from krim_agent.tools.executor import ToolExecutor
from krim_agent.tools.registry import ToolRegistry
from krim_agent.tools.runtime import ToolRuntime, ToolRuntimeConfig
from krim_agent.tools.session import SessionToolScope


class Args(StrictModel):
    value: str = ""


async def run(arguments):
    return {"value": arguments.value}


async def external(invocation, arguments):
    return await run(arguments)


def local_spec():
    return ToolSpec("local_read", Args, run, {"type": "object"}, "1", "a" * 64)


def remote_spec():
    return ToolSpec(
        "remote__read", Args, run, {"type": "object"}, "1", "a" * 64,
        source=ToolSource("mcp", "remote", "read", "b" * 64), external_executor=external,
    )


def corrupt(registry):
    values = {spec.name: spec for spec, _ in registry._frozen}
    name = next(iter(values))
    values[name] = replace(values[name], description="replaced source entry")
    registry._items = MappingProxyType(values)


async def test_host_runtime_derived_registry_detects_original_mapping_replacement():
    base = ToolRegistry((local_spec(),))
    runtime = await ToolRuntime.open(base, ToolRuntimeConfig(), None)
    try:
        assert runtime.registry.schemas() == base.schemas()
        corrupt(base)
        with pytest.raises(ServiceError, match="tool_binding_changed"):
            runtime.registry.assert_current()
    finally:
        await runtime.close()


async def test_duplicate_host_filter_retains_source_freeze_dependencies():
    spec = remote_spec()
    base = ToolExecutor(ToolRegistry((local_spec(), spec)), None)
    host = ToolRuntime(ToolRegistry((spec,)), None)
    scope = SessionToolScope(LOCAL, "local", "session", "turn", "run")

    async def resolve(invocation):
        return scope

    tools = await open_run_tools(base, scope=scope, resolve=resolve, host=host)
    try:
        original_hash = spec.fingerprint
        assert tools.executor.registry.get(spec.name).fingerprint == original_hash
        corrupt(base.registry)
        with pytest.raises(ServiceError, match="tool_binding_changed"):
            tools.schemas()
        with pytest.raises(ServiceError, match="tool_binding_changed"):
            tools.executor.registry.get(spec.name)
    finally:
        await tools.close()
