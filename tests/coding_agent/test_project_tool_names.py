"""Canonical project registrations coexist with source-bound external tools."""
import pytest

from krim_agent.contracts import ServiceError, StrictModel
from krim_agent.tools.core import ToolSource, ToolSpec
from krim_agent.tools.project_names import LEGACY_PROJECT_NAMES, PROJECT_CAPABILITIES
from krim_agent.tools.registry import ToolRegistry


class Empty(StrictModel):
    pass


async def unused(_args):
    return {}


async def scoped(_invocation, _args, _context):
    return {}


async def external(_invocation, _args):
    return {}


def test_generic_mcp_name_is_allowed_but_cannot_replace_private_project_tool():
    native = ToolSpec("write", Empty, unused, {"type": "object"}, "1", "a" * 64,
                      risk="write", scoped_executor=scoped, capability="write")
    remote = ToolSpec("write", Empty, unused, {"type": "object"}, "1", "b" * 64,
                      risk="write", external_executor=external,
                      source=ToolSource("mcp", "fixture", "write", "c" * 64))
    assert remote.descriptor()["policy_version"] == "external/1"
    assert native.descriptor()["policy_version"] == "development/1"
    with pytest.raises(ValueError, match="名称冲突"):
        ToolRegistry((native, remote))


def test_canonical_registry_does_not_resolve_legacy_names():
    specs = tuple(ToolSpec(name, Empty, unused, {"type": "object"}, "1", "a" * 64,
                           risk={"command": "exec", "write": "write"}.get(cap, "read"),
                           scoped_executor=scoped, capability=cap)
                  for name, cap in PROJECT_CAPABILITIES.items())
    registry = ToolRegistry(specs, max_tools=32)
    assert not any(item["name"].startswith("dev_") for item in registry.schemas())
    for old in LEGACY_PROJECT_NAMES:
        with pytest.raises(ServiceError, match="unknown_tool"):
            registry.get(old)
