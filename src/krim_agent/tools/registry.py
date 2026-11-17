"""静态目录：名称冲突拒绝，每次派发核对原定义及 Schema。"""

from types import MappingProxyType

from pydantic import JsonValue

from krim_agent.contracts import ServiceError, fingerprint

from .core import ToolSpec

# Kept as import aliases for callers migrating from the bounded registry.
DEFAULT_MAX_TOOLS = None
MAX_TOOLS = None


class ToolRegistry:
    def __init__(
        self, specs: tuple[ToolSpec, ...], *, max_tools: int | None = None,
        dependencies: tuple["ToolRegistry", ...] = (),
    ) -> None:
        # An explicitly selected legacy limit remains enforceable; normal runtime
        # composition has no product-level tool-count ceiling.
        if max_tools is not None and (type(max_tools) is not int or max_tools < 1):
            raise ValueError("工具目录容量无效")
        if len({item.name for item in specs}) != len(specs):
            raise ValueError("工具名称冲突")
        if max_tools is not None and len(specs) > max_tools:
            raise ServiceError("tool_limit", 422)
        self._items = MappingProxyType({item.name: item for item in specs})
        self._frozen = tuple((item, item.fingerprint) for item in specs)
        self._dependencies = tuple(dependencies)
        if any(not isinstance(item, ToolRegistry) for item in self._dependencies):
            raise ValueError("invalid source registry")

    def _checked_schemas(self) -> list[dict[str, JsonValue]]:
        # Derived registries retain the original freeze fence. Verify each DAG
        # node once even when multiple composed layers share a source registry.
        pending = [self]
        seen: set[int] = set()
        own: list[dict[str, JsonValue]] = []
        descriptors: dict[int, tuple[dict[str, JsonValue], str]] = {}
        while pending:
            registry = pending.pop()
            if id(registry) in seen:
                continue
            seen.add(id(registry))
            schemas = registry._local_schemas(descriptors)
            if registry is self:
                own = schemas
            pending.extend(registry._dependencies)
        return own

    def _local_schemas(
        self, descriptors: dict[int, tuple[dict[str, JsonValue], str]],
    ) -> list[dict[str, JsonValue]]:
        try:
            if len(self._items) != len(self._frozen):
                raise ValueError("tool registry changed")
            schemas = []
            for item, digest in self._frozen:
                if self._items.get(item.name) is not item:
                    raise ValueError("tool definition changed")
                checked = descriptors.get(id(item))
                if checked is None:
                    descriptor = item.descriptor()
                    checked = (descriptor, fingerprint(descriptor))
                    descriptors[id(item)] = checked
                descriptor, current_digest = checked
                if current_digest != digest:
                    raise ValueError("tool definition changed")
                schemas.append(descriptor)
        except (ValueError, TypeError):
            # 登记后损坏 Schema 也是绑定失效，不能泄漏解析异常或换实现。
            raise ServiceError("tool_binding_changed") from None
        return schemas

    def assert_current(self) -> None:
        self._checked_schemas()

    def get(self, name: str) -> ToolSpec:
        self.assert_current()
        if name not in self._items:
            raise ServiceError("unknown_tool")
        return self._items[name]

    def schemas(self) -> list[dict[str, JsonValue]]:
        return self._checked_schemas()
