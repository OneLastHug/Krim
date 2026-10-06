"""同步/异步远端评估 SDK；只消费公开接口，关闭读取不取消生产。"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Iterator
from types import TracebackType

import httpx
from pydantic import BaseModel, TypeAdapter

from .jobs import (
    EvalEvent,
    EvalItemPage,
    EvalJobView,
    EvalPartialReport,
    EvalRemoteResumeRequest,
    EvalRemoteStartRequest,
)
from .types import EvalReport


class RemoteEvalError(ValueError):
    pass


def _parse[T: BaseModel](response: httpx.Response, model: type[T]) -> T:
    if not response.is_success:
        raise RemoteEvalError(f"评估 HTTP 请求失败：{response.status_code}")
    if len(response.content) > 32 * 1024 * 1024:
        raise RemoteEvalError("评估响应超过大小限制")
    return model.model_validate_json(response.content)


def _events(response: httpx.Response) -> list[EvalEvent]:
    if not response.is_success:
        raise RemoteEvalError(f"评估 HTTP 请求失败：{response.status_code}")
    return TypeAdapter(list[EvalEvent]).validate_json(response.content)


class SyncEvalClient:
    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8765",
        api_key: str = "",
        *,
        http: httpx.Client | None = None,
    ) -> None:
        self.http = http or httpx.Client(
            base_url=base_url,
            headers={"Authorization": f"Bearer {api_key}"} if api_key else {},
            timeout=60,
            trust_env=False,
            follow_redirects=False,
        )
        self.owns = http is None

    def __enter__(self) -> SyncEvalClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def close(self) -> None:
        if self.owns:
            self.http.close()

    def start(self, request: EvalRemoteStartRequest) -> EvalJobView:
        return _parse(
            self.http.post("/v1/evals", json=request.model_dump(mode="json")), EvalJobView
        )

    def list(self, *, limit: int = 100) -> list[EvalJobView]:
        response = self.http.get("/v1/evals", params={"limit": limit})
        if not response.is_success:
            raise RemoteEvalError(f"评估 HTTP 请求失败：{response.status_code}")
        return TypeAdapter(list[EvalJobView]).validate_json(response.content)

    def get(self, job_id: str) -> EvalJobView:
        return _parse(self.http.get(f"/v1/evals/{job_id}"), EvalJobView)

    def items(self, job_id: str, *, offset: int = 0, limit: int = 100) -> EvalItemPage:
        return _parse(
            self.http.get(f"/v1/evals/{job_id}/items", params={"offset": offset, "limit": limit}),
            EvalItemPage,
        )

    def report(self, job_id: str) -> EvalReport:
        return _parse(self.http.get(f"/v1/evals/{job_id}/report"), EvalReport)

    def partial_report(self, job_id: str) -> EvalPartialReport:
        return _parse(self.http.get(f"/v1/evals/{job_id}/partial-report"), EvalPartialReport)

    def pause(self, job_id: str) -> EvalJobView:
        return _parse(self.http.post(f"/v1/evals/{job_id}/pause"), EvalJobView)

    def cancel(self, job_id: str) -> EvalJobView:
        return _parse(self.http.post(f"/v1/evals/{job_id}/cancel"), EvalJobView)

    def resume(self, job_id: str, request: EvalRemoteResumeRequest | None = None) -> EvalJobView:
        return _parse(
            self.http.post(
                f"/v1/evals/{job_id}/resume",
                json=(request or EvalRemoteResumeRequest()).model_dump(mode="json"),
            ),
            EvalJobView,
        )

    def wait(self, job_id: str, *, timeout_seconds: float = 300) -> EvalJobView:
        deadline = time.monotonic() + timeout_seconds
        while True:
            value = self.get(job_id)
            if value.status not in {"queued", "running"} or value.can_resume:
                return value
            if time.monotonic() >= deadline:
                raise TimeoutError("等待读取超时；后台任务继续运行")
            time.sleep(0.1)

    def events(self, job_id: str, from_seq: int = 0) -> Iterator[EvalEvent]:
        cursor = from_seq
        while True:
            records = _events(
                self.http.get(f"/v1/evals/{job_id}/events", params={"from_seq": cursor})
            )
            for record in records:
                if record.job_id != job_id or record.seq != cursor:
                    raise RemoteEvalError("评估事件来源或序号不正确")
                cursor += 1
                yield record
            value = self.get(job_id)
            if (
                value.status not in {"queued", "running"} or value.can_resume
            ) and cursor >= value.event_count:
                return
            time.sleep(0.05)


class AsyncEvalClient:
    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8765",
        api_key: str = "",
        *,
        http: httpx.AsyncClient | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.http = http or httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": f"Bearer {api_key}"} if api_key else {},
            timeout=60,
            trust_env=False,
            follow_redirects=False,
            transport=transport,
        )
        self.owns = http is None

    async def __aenter__(self) -> AsyncEvalClient:
        return self

    async def __aexit__(
        self,
        _type: type[BaseException] | None,
        _value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        if self.owns:
            await self.http.aclose()

    async def start(self, request: EvalRemoteStartRequest) -> EvalJobView:
        return _parse(
            await self.http.post("/v1/evals", json=request.model_dump(mode="json")), EvalJobView
        )

    async def list(self, *, limit: int = 100) -> list[EvalJobView]:
        response = await self.http.get("/v1/evals", params={"limit": limit})
        if not response.is_success:
            raise RemoteEvalError(f"评估 HTTP 请求失败：{response.status_code}")
        return TypeAdapter(list[EvalJobView]).validate_json(response.content)

    async def get(self, job_id: str) -> EvalJobView:
        return _parse(await self.http.get(f"/v1/evals/{job_id}"), EvalJobView)

    async def items(self, job_id: str, *, offset: int = 0, limit: int = 100) -> EvalItemPage:
        return _parse(
            await self.http.get(
                f"/v1/evals/{job_id}/items", params={"offset": offset, "limit": limit}
            ),
            EvalItemPage,
        )

    async def report(self, job_id: str) -> EvalReport:
        return _parse(await self.http.get(f"/v1/evals/{job_id}/report"), EvalReport)

    async def partial_report(self, job_id: str) -> EvalPartialReport:
        return _parse(await self.http.get(f"/v1/evals/{job_id}/partial-report"), EvalPartialReport)

    async def pause(self, job_id: str) -> EvalJobView:
        return _parse(await self.http.post(f"/v1/evals/{job_id}/pause"), EvalJobView)

    async def cancel(self, job_id: str) -> EvalJobView:
        return _parse(await self.http.post(f"/v1/evals/{job_id}/cancel"), EvalJobView)

    async def resume(
        self, job_id: str, request: EvalRemoteResumeRequest | None = None
    ) -> EvalJobView:
        return _parse(
            await self.http.post(
                f"/v1/evals/{job_id}/resume",
                json=(request or EvalRemoteResumeRequest()).model_dump(mode="json"),
            ),
            EvalJobView,
        )

    async def wait(self, job_id: str, *, timeout_seconds: float = 300) -> EvalJobView:
        async with asyncio.timeout(timeout_seconds):
            while True:
                value = await self.get(job_id)
                if value.status not in {"queued", "running"} or value.can_resume:
                    return value
                await asyncio.sleep(0.1)

    async def events(self, job_id: str, from_seq: int = 0) -> AsyncIterator[EvalEvent]:
        cursor = from_seq
        while True:
            records = _events(
                await self.http.get(f"/v1/evals/{job_id}/events", params={"from_seq": cursor})
            )
            for record in records:
                if record.job_id != job_id or record.seq != cursor:
                    raise RemoteEvalError("评估事件来源或序号不正确")
                cursor += 1
                yield record
            value = await self.get(job_id)
            if (
                value.status not in {"queued", "running"} or value.can_resume
            ) and cursor >= value.event_count:
                return
            await asyncio.sleep(0.05)
