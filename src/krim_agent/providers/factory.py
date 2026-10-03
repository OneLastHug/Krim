"""显式配置的 Python provider 装配；创建本身不发送网络请求。"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .fake import FakeProvider
from .openai_compatible import OpenAICompatibleConfig, OpenAICompatibleProvider

if TYPE_CHECKING:
    from krim_agent.config import Settings

OwnedProvider = FakeProvider | OpenAICompatibleProvider


def create_provider(settings: Settings) -> OwnedProvider:
    if settings.provider == "fake":
        return FakeProvider()
    return OpenAICompatibleProvider(
        OpenAICompatibleConfig(
            base_url=settings.provider_base_url or "",
            model=settings.provider_model or "",
            api_key=settings.provider_api_key,
            timeout_seconds=settings.provider_timeout_seconds,
            max_output_tokens=settings.provider_max_output_tokens,
        )
    )
