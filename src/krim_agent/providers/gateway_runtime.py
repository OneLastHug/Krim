"""为可信宿主临时启动本机预算网关；不读取凭据或选择上游。"""

from __future__ import annotations

import asyncio
import socket
from collections import deque
from collections.abc import Iterator
from contextlib import contextmanager
from types import TracebackType

import httpx
import uvicorn
from pydantic import JsonValue
from starlette.types import Message, Receive, Scope, Send

from krim_agent.contracts import ServiceError
from krim_agent.runtime.contracts import settled

from .budget_gateway import GatewayConfig, create_gateway


class _LocalServer(uvicorn.Server):
    @contextmanager
    def capture_signals(self) -> Iterator[None]:
        yield


class LocalBudgetGateway:
    """单次生命周期、随机回环端口；计量仍归原create_gateway与持久账本。"""

    def __init__(
        self,
        config: GatewayConfig,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        capture_streams: bool = False,
    ) -> None:
        self._app = create_gateway(config, transport=transport)
        self._capture_streams = capture_streams
        self._responses: deque[dict[str, JsonValue]] = deque(maxlen=8)
        self._socket: socket.socket | None = None
        self._server: _LocalServer | None = None
        self._task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._base_url: str | None = None
        self._used = False
        self._closed = True

    @property
    def base_url(self) -> str:
        if self._base_url is None or self._closed:
            raise ServiceError("closed")
        return self._base_url

    @property
    def resources(self) -> dict[str, bool]:
        return {
            "closed": self._closed,
            "server_joined": self._task is None or self._task.done(),
            "socket_closed": self._socket is None or self._socket.fileno() == -1,
        }

    @property
    def responses(self) -> list[dict[str, JsonValue]]:
        return [dict(value) for value in self._responses]

    async def _serve_app(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        path = scope.get("path", "")
        path = path if path in {
            "/models", "/v1/models", "/chat/completions", "/v1/chat/completions"
        } else "/unsupported"
        entry: dict[str, JsonValue] = {"path": path, "status": None, "content_type": ""}
        wire = bytearray()
        size = 0

        async def observed(message: Message) -> None:
            nonlocal size
            if message["type"] == "http.response.start":
                entry["status"] = message["status"]
                entry["content_type"] = next(
                    (value.decode("ascii", errors="replace") for name, value in
                     message.get("headers", []) if name.lower() == b"content-type"), ""
                )
            elif message["type"] == "http.response.body":
                body = message.get("body", b"")
                size += len(body)
                if self._capture_streams and str(entry["content_type"]).startswith(
                    "text/event-stream"
                ):
                    wire.extend(body[:max(0, 131072 - len(wire))])
            await send(message)

        try:
            await self._app(scope, receive, observed)
        finally:
            entry["response_bytes"] = size
            if self._capture_streams and wire:
                # 仅保留create_gateway脱敏后的下游SSE；不读取请求或认证头。
                entry["stream_body"] = wire.decode("utf-8", errors="replace")
                entry["capture_truncated"] = size > len(wire)
            self._responses.append(entry)

    async def __aenter__(self) -> LocalBudgetGateway:
        if self._used:
            raise ServiceError("closed")
        self._used = True
        self._closed = False
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket = listener
        try:
            listener.bind(("127.0.0.1", 0))
            listener.listen(32)
            listener.setblocking(False)
            port = listener.getsockname()[1]
            self._base_url = f"http://127.0.0.1:{port}"
            self._server = _LocalServer(
                uvicorn.Config(
                    self._serve_app,
                    interface="asgi3",
                    host="127.0.0.1",
                    port=port,
                    access_log=False,
                    log_config=None,
                    log_level="error",
                    lifespan="on",
                    ws="none",
                    timeout_keep_alive=1,
                    timeout_graceful_shutdown=5,
                )
            )
            self._task = asyncio.create_task(self._server.serve(sockets=[listener]))
            async with asyncio.timeout(5):
                while not self._server.started:
                    if self._task.done():
                        await self._task
                        raise ServiceError("gateway_start_failed", 503)
                    await asyncio.sleep(0.01)
            return self
        except BaseException:
            await self.close()
            raise

    async def __aexit__(
        self,
        kind: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        self._used = True
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await settled(self._close_task)

    async def _close(self) -> None:
        try:
            if self._server is not None:
                self._server.should_exit = True
            if self._task is not None:
                await self._task
        finally:
            if self._socket is not None:
                self._socket.close()
            self._base_url = None
            self._closed = (
                getattr(self._app.state, "gateway_client", None) is None
                and getattr(self._app.state, "gateway_ledger", None) is None
            )
