"""Locale（区域标签）规范化与确定性回落；不把语言选择当作鉴权。"""

from __future__ import annotations

from typing import Literal

import langcodes
from pydantic import Field, field_validator

from .protocol.models import StrictModel

LocaleTag = Literal["zh-CN", "en-US"]
DEFAULT_LOCALE: LocaleTag = "zh-CN"
SUPPORTED_LOCALES: tuple[LocaleTag, ...] = ("zh-CN", "en-US")


class LocaleChange(StrictModel):
    locale: str = Field(min_length=2, max_length=64)

    @field_validator("locale")
    @classmethod
    def valid_tag(cls, value: str) -> str:
        return normalize_locale(value)


class LocaleView(StrictModel):
    requested: str
    effective: LocaleTag
    fallback_chain: list[str]


def normalize_locale(value: str) -> str:
    raw = value.strip().replace("_", "-")
    if not 2 <= len(raw) <= 64:
        raise ValueError("locale 必须是有限的 BCP 47 标签")
    try:
        normalized = langcodes.standardize_tag(raw)
        if not langcodes.tag_is_valid(normalized):
            raise ValueError
    except ValueError:
        raise ValueError("locale 必须是有效的 BCP 47 标签") from None
    return normalized


def resolve_locale(
    requested: str | None,
    *,
    supported: tuple[LocaleTag, ...] = SUPPORTED_LOCALES,
    default: LocaleTag = DEFAULT_LOCALE,
) -> LocaleView:
    if default not in supported:
        raise ValueError("默认 Locale 必须具有实际资源")
    normalized = normalize_locale(default if requested is None else requested)
    candidates = [normalized]
    parts = normalized.split("-")
    # 去掉尾部子标签时同时去掉孤立 singleton，避免输出 en-US-u 这类非法候选。
    while len(parts) > 1:
        parts.pop()
        if len(parts[-1]) == 1:
            parts.pop()
        if parts:
            candidate = "-".join(parts)
            if langcodes.tag_is_valid(candidate):
                candidates.append(candidate)
    language = langcodes.Language.get(normalized).language or ""
    candidates.extend(
        tag
        for tag in supported
        if tag.lower().startswith(language.lower() + "-") and tag not in candidates
    )
    if default not in candidates:
        candidates.append(default)
    effective = next((tag for tag in candidates if tag in supported), default)
    return LocaleView(requested=normalized, effective=effective, fallback_chain=candidates)
