"""类型化模型适配接口；默认离线，真实端点须显式配置。"""

from .factory import create_provider
from .fake import FakeProvider
from .openai_compatible import OpenAICompatibleConfig, OpenAICompatibleProvider, ProviderError
from .types import ModelProvider, ModelText, ModelToolCall, ModelUsage

__all__ = [
    "FakeProvider",
    "ModelProvider",
    "ModelText",
    "ModelToolCall",
    "ModelUsage",
    "OpenAICompatibleConfig",
    "OpenAICompatibleProvider",
    "ProviderError",
    "create_provider",
]
