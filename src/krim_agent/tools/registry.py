"""静态目录：名称冲突拒绝，每次派发核对原定义及 Schema。"""

from types import MappingProxyType

from pydantic import JsonValue

from krim_agent.contracts import ServiceError

from .core import ToolSpec

DEFAULT_MAX_TOOLS = 16
MAX_TOOLS = 32


class ToolRegistry:
    def __init__(self, specs: tuple[ToolSpec, ...], *, max_tools: int = DEFAULT_MAX_TOOLS) -> None:
        if type(max_tools) is not int or not DEFAULT_MAX_TOOLS <= max_tools <= MAX_TOOLS:
            raise ValueError("工具目录容量无效")
        if len({item.name for item in specs}) != len(specs):
            raise ValueError("工具名称冲突")
        if len(specs) > max_tools:
            raise ServiceError("tool_limit", 422)
        self._items = MappingProxyType({item.name: item for item in specs})
        self._frozen = tuple((item, item.fingerprint) for item in specs)

    def assert_current(self) -> None:
        try:
            changed = len(self._items) != len(self._frozen) or any(
                self._items.get(item.name) is not item or item.fingerprint != digest
                for item, digest in self._frozen
            )
        except (ValueError, TypeError):
            # 登记后损坏 Schema 也是绑定失效，不能泄漏解析异常或换实现。
            raise ServiceError("tool_binding_changed") from None
        if changed:
            raise ServiceError("tool_binding_changed")

    def get(self, name: str) -> ToolSpec:
        self.assert_current()
        if name not in self._items:
            raise ServiceError("unknown_tool")
        return self._items[name]

    def schemas(self) -> list[dict[str, JsonValue]]:
        self.assert_current()
        return [item.descriptor() for item, _ in self._frozen]
