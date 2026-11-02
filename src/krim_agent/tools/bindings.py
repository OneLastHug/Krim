"""构造期解析显式覆盖，不修改现有静态目录。"""

from collections.abc import Mapping
from dataclasses import replace

from krim_agent.contracts import ServiceError

from .core import ToolSpec
from .registry import ToolRegistry


def resolve_tool_overrides(
    specs: tuple[ToolSpec, ...],
    overrides: Mapping[str, str],
    public_tools: tuple[str, ...] | None = None,
) -> ToolRegistry:
    catalog = {spec.name: spec for spec in specs}
    if len(catalog) != len(specs) or len(overrides) > 16:
        raise ServiceError("invalid_input", 422)
    names = tuple(catalog) if public_tools is None else public_tools
    if len(names) != len(set(names)) or set(names) - set(catalog) or set(overrides) - set(names):
        raise ServiceError("invalid_input", 422)
    result = []
    for name in names:
        original = catalog[name]
        target_name = overrides.get(name)
        if target_name is None:
            result.append(original)
            continue
        target = catalog.get(target_name)
        if (
            not original.overridable
            or original.scoped_executor is not None
            or target is None
            or target is original
            or target.binding_target is not None
            or target.source is None
            or target.source.kind != "mcp"
        ):
            raise ServiceError("invalid_input", 422)
        result.append(replace(target, name=name, binding_target=target, overridable=False))
    return ToolRegistry(tuple(result))
