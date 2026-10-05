"""SRC-OSS-008：OpenTelemetry Context 校验与受控 OTLP HTTP/JSON 导出。"""

from __future__ import annotations

from urllib.parse import urlsplit

import httpx
from pydantic import SecretStr

from krim_agent.observability import SpanRecord


class OtlpJsonSink:
    def __init__(
        self,
        endpoint: str,
        *,
        headers: dict[str, SecretStr] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_seconds: float = 2,
    ) -> None:
        url = urlsplit(endpoint)
        if (
            url.scheme not in {"http", "https"}
            or not url.hostname
            or url.username
            or url.password
            or url.query
            or url.fragment
            or (url.scheme == "http" and url.hostname not in {"localhost", "127.0.0.1", "::1"})
        ):
            raise ValueError("OTLP 需要显式 HTTPS 或本机 HTTP 端点，禁止凭证/query/fragment")
        self._endpoint = endpoint
        self._http = httpx.AsyncClient(
            transport=transport,
            timeout=timeout_seconds,
            trust_env=False,
            follow_redirects=False,
            headers={name: value.get_secret_value() for name, value in (headers or {}).items()},
        )

    async def export(self, record: SpanRecord) -> None:
        from opentelemetry.trace import SpanContext, TraceFlags

        record = SpanRecord.model_validate(record.model_dump())
        if record.end_ns is None:
            return
        context = SpanContext(
            int(record.trace_id, 16), int(record.span_id, 16), False, TraceFlags(TraceFlags.SAMPLED)
        )
        if not context.is_valid:
            raise ValueError("OTLP 标识不满足 W3C trace/span 非零要求")
        attributes = []
        for key, value in record.attributes.items():
            field = (
                "boolValue"
                if type(value) is bool
                else "intValue"
                if type(value) is int
                else "stringValue"
            )
            attributes.append(
                {"key": key, "value": {field: str(value) if field == "intValue" else value}}
            )
        span = {
            "traceId": record.trace_id,
            "spanId": record.span_id,
            "name": "krim." + record.kind,
            "kind": 1,
            "startTimeUnixNano": str(record.start_ns),
            "endTimeUnixNano": str(record.end_ns),
            "attributes": attributes,
            "status": {"code": 1 if record.status == "ok" else 2},
        }
        if record.parent_span_id is not None:
            span["parentSpanId"] = record.parent_span_id
        payload = {
            "resourceSpans": [
                {
                    "resource": {
                        "attributes": [
                            {"key": "service.name", "value": {"stringValue": "Krim Agent"}}
                        ]
                    },
                    "scopeSpans": [
                        {"scope": {"name": "krim_agent", "version": "0.1.0.dev0"}, "spans": [span]}
                    ],
                }
            ]
        }
        response = await self._http.post(self._endpoint, json=payload)
        if response.status_code != 200:
            raise RuntimeError("OTLP Collector 没有成功接收")
        if response.content:
            body = response.json()
            if body.get("partialSuccess", {}).get("rejectedSpans", 0) not in (0, "0"):
                raise RuntimeError("OTLP Collector 部分拒绝 spans")

    async def close(self) -> None:
        await self._http.aclose()
