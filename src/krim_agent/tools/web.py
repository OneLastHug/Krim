"""当前 ToolSpec 协议下的内置 HTTP 网页工具。"""

from __future__ import annotations

import inspect
from typing import Annotated, cast

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from krim_agent.contracts import ServiceError, StrictModel, fingerprint
from krim_agent.source_fingerprints import source_digest
from krim_agent.web_access import TavilyWebAccess

from .core import Invocation, ToolSource, ToolSpec


class SearchInput(StrictModel):
    query: Annotated[str, Field(min_length=1, max_length=1000)]
    max_results: Annotated[int, Field(ge=1, le=10)] = 5


class FetchInput(StrictModel):
    model_config = ConfigDict(
        json_schema_extra={
            "oneOf": [
                {"required": ["url"], "not": {"required": ["result_ref"]}},
                {"required": ["result_ref"], "not": {"required": ["url"]}},
            ]
        }
    )
    url: Annotated[str, Field(min_length=1, max_length=2048)] | None = None
    result_ref: Annotated[str, Field(min_length=1, max_length=128)] | None = None
    offset: Annotated[int, Field(ge=0)] = 0
    max_chars: Annotated[int, Field(ge=1, le=15000)] = 15000

    @model_validator(mode="after")
    def require_url_or_reference(self) -> FetchInput:
        if (self.url is None) == (self.result_ref is None):
            raise ValueError("provide exactly one of url or result_ref")
        return self


_RESULT: dict[str, JsonValue] = {
    "type": "object",
    "required": ["ok", "operation", "results", "usage", "request_id", "truncated"],
    "properties": {
        "ok": {"const": True},
        "operation": {"enum": ["search", "fetch"]},
        "results": {
            "type": "array",
            "maxItems": 10,
            "items": {
                "type": "object",
                "required": [
                    "title", "url", "content", "result_ref", "total_chars", "next_offset"
                ],
                "properties": {
                    "title": {"type": "string"},
                    "url": {"type": "string"},
                    "content": {"type": "string"},
                    "result_ref": {"type": "string"},
                    "total_chars": {"type": "integer", "minimum": 0},
                    "next_offset": {"type": ["integer", "null"]},
                },
                "additionalProperties": False,
            },
        },
        "usage": True,
        "request_id": {"type": ["string", "null"]},
        "truncated": {"type": "boolean"},
    },
    "additionalProperties": False,
}


async def _without_invocation(arguments: BaseModel) -> JsonValue:
    raise ServiceError("tool_binding_changed")


def web_tools(backend: TavilyWebAccess) -> tuple[ToolSpec, ...]:
    async def search(invocation: Invocation, arguments: BaseModel) -> JsonValue:
        values = cast(SearchInput, arguments)
        return await backend.search(values.query, values.max_results, scope=invocation.run_id)

    async def fetch(invocation: Invocation, arguments: BaseModel) -> JsonValue:
        values = cast(FetchInput, arguments)
        return await backend.fetch(
            values.url,
            values.max_chars,
            result_ref=values.result_ref,
            offset=values.offset,
            scope=invocation.run_id,
        )

    digest = fingerprint(
        {
            "adapter": source_digest(__file__),
            "configuration": backend.configuration_digest,
            "client": source_digest(inspect.getfile(type(backend))),
        }
    )
    return (
        ToolSpec(
            "web_search",
            SearchInput,
            _without_invocation,
            _RESULT,
            "tavily/2",
            digest,
            source=ToolSource("http", "tavily", "search", backend.configuration_digest),
            external_executor=search,
            overridable=True,
            description="搜索公开网页，返回来源、正文片段和可继续读取的result_ref。",
        ),
        ToolSpec(
            "web_fetch",
            FetchInput,
            _without_invocation,
            _RESULT,
            "tavily/2",
            digest,
            source=ToolSource("http", "tavily", "extract", backend.configuration_digest),
            external_executor=fetch,
            overridable=True,
            description=(
                "通过第三方 API 读取公开网页；使用result_ref和offset读取已保存正文，"
                "不会为分页重新请求来源。"
            ),
        ),
    )
