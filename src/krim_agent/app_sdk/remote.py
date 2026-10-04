"""Python Host SDK：经已鉴权 HTTP/SSE 控制真实同源 iframe，不把入队当回执。"""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Literal
from uuid import uuid4

import httpx
from pydantic import JsonValue

from krim_agent.host import (
    HostCommand,
    HostSessionView,
    HostSidebarResult,
    HostThemeResult,
    HostUIState,
    HostWidthResult,
)
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import ChannelResult, ProtocolEnvelope, StreamFrame

HostNotification = Callable[[JsonValue], object | Awaitable[object]]


class RemoteHostHandle:
    def __init__(
        self,
        base_url: str,
        session_id: str,
        api_key: str = "",
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.session_id = session_id
        self._http = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": f"Bearer {api_key}"} if api_key else {},
            timeout=12,
            trust_env=False,
            follow_redirects=False,
            transport=transport,
        )
        self._closed = False
        self._handlers: dict[str, HostNotification] = {}

    async def __aenter__(self) -> RemoteHostHandle:
        await self.status()
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()

    def _path(self, suffix: str = "") -> str:
        return f"/v1/host/sessions/{self.session_id}{suffix}"

    @staticmethod
    def _check(response: httpx.Response) -> None:
        if response.is_success:
            return
        try:
            error = response.json()["error"]
            raise ServiceError(str(error["code"]), str(error["message"]), response.status_code)
        except (KeyError, ValueError, TypeError):
            raise ServiceError(
                "host_http_error", "宿主控制请求失败", response.status_code
            ) from None

    async def status(self) -> HostSessionView:
        if self._closed:
            raise ConnectionError("RemoteHostHandle 已关闭")
        response = await self._http.get(self._path())
        self._check(response)
        return HostSessionView.model_validate_json(response.content)

    async def emit(
        self,
        channel: str,
        data: JsonValue = None,
        *,
        timeout: float = 10,  # noqa: ASYNC109
    ) -> ChannelResult:
        if self._closed:
            raise ConnectionError("RemoteHostHandle 已关闭")
        command = HostCommand(
            envelope=ProtocolEnvelope(kind="call", id=str(uuid4()), channel=channel, data=data),
            timeout_seconds=timeout,
        )
        response = await self._http.post(
            self._path("/commands"),
            json=command.model_dump(mode="json", exclude_none=True),
            timeout=timeout + 2,
        )
        self._check(response)
        return ChannelResult.model_validate_json(response.content)

    async def _call(self, channel: str, data: JsonValue = None) -> JsonValue:
        result = await self.emit(channel, data)
        if not result.ok:
            raise ServiceError("host_channel_error", result.error or "宿主 UI 调用失败")
        return result.data

    async def state(self) -> HostUIState:
        return HostUIState.model_validate(await self._call("host/get-state"))

    async def set_theme(self, theme: Literal["light", "dark", "system"]) -> HostThemeResult:
        return HostThemeResult.model_validate(await self._call("host/theme", {"theme": theme}))

    async def resize(self, width: int) -> HostWidthResult:
        return HostWidthResult.model_validate(await self._call("host/resize", {"width": width}))

    async def sidebar(self, open: bool) -> HostSidebarResult:
        return HostSidebarResult.model_validate(await self._call("host/sidebar", {"open": open}))

    async def prompt(
        self, value: str, *, mode: Literal["replace", "append"] = "replace"
    ) -> JsonValue:
        return await self._call("host/prompt", {"value": value, "mode": mode})

    async def notice(self, message: str) -> JsonValue:
        return await self._call("host/notice", {"message": message})

    def on(self, channel: str, handler: HostNotification) -> Callable[[], None]:
        if channel in self._handlers:
            raise ValueError("宿主通知 handler 已注册")
        self._handlers[channel] = handler

        def remove() -> None:
            if self._handlers.get(channel) is handler:
                self._handlers.pop(channel, None)

        return remove

    async def events(self, from_seq: int = 0) -> AsyncIterator[ProtocolEnvelope]:
        if self._closed:
            raise ConnectionError("RemoteHostHandle 已关闭")
        cursor = from_seq
        async with self._http.stream(
            "GET",
            self._path("/events"),
            params={"side": "host", "from_seq": from_seq},
            timeout=httpx.Timeout(20, read=20),
        ) as response:
            self._check(response)
            async for line in response.aiter_lines():
                if not line.startswith("data: "):
                    continue
                frame = StreamFrame.model_validate_json(line[6:])
                if frame.stream_id != self.session_id or frame.seq != cursor:
                    raise ServiceError("host_stream_invalid", "宿主事件关联或游标无效")
                cursor += 1
                envelope = ProtocolEnvelope.model_validate(frame.data)
                if envelope.kind != "notify":
                    raise ServiceError("host_stream_invalid", "宿主事件不是通知")
                handler = self._handlers.get(envelope.channel)
                if handler is not None:
                    result = handler(envelope.data)
                    if inspect.isawaitable(result):
                        await result
                yield envelope

    async def destroy(self) -> None:
        if self._closed:
            return
        response = await self._http.delete(self._path())
        self._check(response)
        await self.close()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._handlers.clear()
        await self._http.aclose()
