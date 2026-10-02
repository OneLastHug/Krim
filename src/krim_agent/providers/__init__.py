"""类型化模型适配接口；M1 只有离线 fake provider。"""

from .fake import FakeProvider
from .types import ModelProvider, ModelText, ModelToolCall

__all__ = ["FakeProvider", "ModelProvider", "ModelText", "ModelToolCall"]
