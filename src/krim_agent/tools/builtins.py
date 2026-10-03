"""M1 离线示例工具：不执行文件、Shell 或网络操作。"""

from .core import tool
from .packs import BuiltinPackState, builtin_pack
from .registry import ToolRegistry


@tool(description="计算两个有限数字的和", risk="read")
async def add(left: float, right: float) -> dict[str, float]:
    return {"sum": left + right}


@tool(description="原样回显输入文本", risk="read")
async def echo(text: str = "") -> str:
    return text


def builtin_registry(
    timeout: float = 10, *, pack_state: BuiltinPackState | None = None
) -> ToolRegistry:
    registry = ToolRegistry(default_timeout=timeout)
    registry.register_many(add, echo)
    pack, _state = builtin_pack(pack_state)
    registry.register_many(*pack)
    return registry
