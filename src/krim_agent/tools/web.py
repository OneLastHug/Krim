"""当前 ToolSpec 协议下的内置 HTTP 网页工具。"""

import inspect
from typing import Annotated, cast

from pydantic import BaseModel, Field, JsonValue

from krim_agent.contracts import ServiceError, StrictModel, fingerprint
from krim_agent.source_fingerprints import source_digest
from krim_agent.web_access import TavilyWebAccess

from .core import Invocation, ToolSource, ToolSpec


class SearchInput(StrictModel):
    query: Annotated[str, Field(min_length=1, max_length=1000)]
    max_results: Annotated[int, Field(ge=1, le=10)] = 5


class FetchInput(StrictModel):
    url: Annotated[str, Field(min_length=1, max_length=2048)]
    max_chars: Annotated[int, Field(ge=1, le=15000)] = 15000


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
                "required": ["title", "url", "content"],
                "properties": {key: {"type": "string"} for key in ("title", "url", "content")},
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
        return await backend.search(values.query, values.max_results)

    async def fetch(invocation: Invocation, arguments: BaseModel) -> JsonValue:
        values = cast(FetchInput, arguments)
        return await backend.fetch(values.url, values.max_chars)

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
            "tavily/1",
            digest,
            source=ToolSource("http", "tavily", "search", backend.configuration_digest),
            external_executor=search,
            overridable=True,
            description="搜索公开网页，返回带来源 URL 的片段。",
        ),
        ToolSpec(
            "web_fetch",
            FetchInput,
            _without_invocation,
            _RESULT,
            "tavily/1",
            digest,
            source=ToolSource("http", "tavily", "extract", backend.configuration_digest),
            external_executor=fetch,
            overridable=True,
            description="通过第三方 API 读取公开网页；超长正文标记截断。",
        ),
    )
