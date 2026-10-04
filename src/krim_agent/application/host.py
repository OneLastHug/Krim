"""当前进程的 Python Host 回程；UI 回执之前不把命令入队当成功。"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from uuid import uuid4

from pydantic import JsonValue, TypeAdapter

from krim_agent.host import HostCommand, HostSessionView, HostSide
from krim_agent.link import LinkNavigate
from krim_agent.protocol.domain import ServiceError
from krim_agent.protocol.models import ChannelResult, ProtocolEnvelope, StreamFrame

HOST_UI_CHANNELS = frozenset(
    {
        "host/get-state",
        "host/request",
        "host/set-locale",
        "host/resize",
        "host/prompt",
        "host/notice",
        "host/theme",
        "host/sidebar",
        "mention/get",
        "mention/add",
        "mention/remove",
        "mention/clear",
        "drag/enter",
        "drag/leave",
        "drag/drop",
        "context/getTags",
        "context/addTag",
        "context/removeTag",
        "context/clearTags",
        "skill/list",
        "skill/inject",
        "skill/remove",
        "skill/read",
    }
)

HOST_CALLBACK_CHANNELS = frozenset(
    {
        "checkpoint/create",
        "checkpoint/rollback",
        "checkpoint/canRollback",
        "changeList/invoke",
        "host/callback",
    }
)


@dataclass
class PendingHostCall:
    channel: str
    target: HostSide
    future: asyncio.Future[ChannelResult]


@dataclass
class HostSession:
    id: str = field(default_factory=lambda: str(uuid4()))
    connected: bool = False
    closed: bool = False
    expires_at: float = field(default_factory=lambda: time.time() + 3600)
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    pending: dict[str, PendingHostCall] = field(default_factory=dict)
    seen: set[str] = field(default_factory=set)
    delivered: set[str] = field(default_factory=set)
    notifications: set[str] = field(default_factory=set)
    frames: dict[HostSide, list[StreamFrame]] = field(
        default_factory=lambda: {"host": [], "app": []}
    )

    def view(self) -> HostSessionView:
        return HostSessionView(
            id=self.id, connected=self.connected, closed=self.closed, expires_at=self.expires_at
        )


class HostSessions:
    def __init__(self, *, max_sessions: int = 32, max_pending: int = 128) -> None:
        self.max_sessions = max_sessions
        self.max_pending = max_pending
        self._sessions: dict[str, HostSession] = {}
        self._lock = asyncio.Lock()

    async def create(self) -> HostSessionView:
        async with self._lock:
            stale = [
                key
                for key, value in self._sessions.items()
                if value.closed or value.expires_at <= time.time()
            ]
            for key in stale:
                await self.close(key)
                self._sessions.pop(key, None)
            if len(self._sessions) >= self.max_sessions:
                raise ServiceError("host_session_limit", "宿主控制会话超过上限", 429)
            session = HostSession()
            self._sessions[session.id] = session
            return session.view()

    def session(self, identifier: str) -> HostSession:
        value = self._sessions.get(identifier)
        if value is None:
            raise ServiceError("host_session_not_found", "宿主控制会话不存在", 404)
        if value.closed or value.expires_at <= time.time():
            raise ServiceError("host_session_closed", "宿主控制会话已关闭或过期", 410)
        return value

    async def mark_ready(self, identifier: str) -> HostSessionView:
        session = self.session(identifier)
        session.connected = True
        await self.publish(
            session,
            "host",
            ProtocolEnvelope(
                kind="notify", id=str(uuid4()), channel="host/ready", data={"connected": True}
            ),
        )
        return session.view()

    async def publish(
        self, session: HostSession, target: HostSide, envelope: ProtocolEnvelope
    ) -> None:
        async with session.condition:
            frames = session.frames[target]
            if len(frames) >= 512:
                raise ServiceError("host_event_limit", "宿主控制会话事件超过上限", 429)
            frames.append(
                StreamFrame(
                    stream_id=session.id,
                    seq=len(frames),
                    kind="chunk",
                    data=envelope.model_dump(mode="json", exclude={"stream_id", "seq"}),
                )
            )
            session.condition.notify_all()

    async def command(self, identifier: str, command: HostCommand) -> ChannelResult:
        return await self._request(identifier, command, "app")

    async def request(self, identifier: str, command: HostCommand) -> ChannelResult:
        if command.envelope.kind != "call":
            raise ServiceError("host_channel_invalid", "App 回程必须是 call", 422)
        return await self._request(identifier, command, "host")

    async def _request(
        self, identifier: str, command: HostCommand, target: HostSide
    ) -> ChannelResult:
        session = self.session(identifier)
        envelope = command.envelope
        allowed = HOST_UI_CHANNELS if target == "app" else HOST_CALLBACK_CHANNELS
        if envelope.kind not in {"call", "notify"} or envelope.channel not in allowed:
            raise ServiceError("host_channel_invalid", "宿主控制信道或方向无效", 422)
        if not session.connected:
            raise ServiceError("host_not_ready", "宿主 iframe 尚未就绪", 409)
        async with session.condition:
            if envelope.id in session.seen:
                raise ServiceError("host_duplicate_id", "宿主请求 ID 已使用", 409)
            if len(session.pending) >= self.max_pending:
                raise ServiceError("host_pending_limit", "宿主待回执请求超过上限", 429)
            if len(session.seen) >= 512:
                raise ServiceError("host_request_limit", "宿主会话请求超过上限", 429)
            session.seen.add(envelope.id)
            future: asyncio.Future[ChannelResult] | None = None
            if envelope.kind == "call":
                future = asyncio.get_running_loop().create_future()
                session.pending[envelope.id] = PendingHostCall(envelope.channel, target, future)
        try:
            await self.publish(session, target, envelope)
            if envelope.kind == "notify":
                return ChannelResult(ok=True, data=None)
            async with asyncio.timeout(command.timeout_seconds):
                assert future is not None
                return await future
        except TimeoutError as error:
            raise ServiceError("host_timeout", "宿主执行回执超时", 504) from error
        finally:
            session.pending.pop(envelope.id, None)
            if future is not None and not future.done():
                future.cancel()
            if future is not None and future.cancelled() and not session.closed:
                await self.publish(
                    session,
                    target,
                    ProtocolEnvelope(kind="cancel", id=envelope.id, channel=envelope.channel),
                )

    async def cancel(self, identifier: str, request_id: str) -> ChannelResult:
        session = self.session(identifier)
        async with session.condition:
            pending = session.pending.get(request_id)
            if pending is None:
                raise ServiceError("host_reply_gone", "宿主请求已处理或不存在", 409)
            if not pending.future.done():
                pending.future.set_exception(ServiceError("host_cancelled", "宿主请求已取消", 409))
        await self.publish(
            session,
            pending.target,
            ProtocolEnvelope(kind="cancel", id=request_id, channel=pending.channel),
        )
        return ChannelResult(ok=True)

    async def reply(
        self, identifier: str, envelope: ProtocolEnvelope, side: HostSide = "app"
    ) -> ChannelResult:
        session = self.session(identifier)
        if envelope.kind != "reply":
            raise ServiceError("host_reply_invalid", "需要宿主回复信封", 422)
        try:
            result = ChannelResult.model_validate(envelope.data)
        except ValueError as error:
            raise ServiceError("host_reply_invalid", "宿主回执结构无效", 422) from error
        async with session.condition:
            pending = session.pending.get(envelope.id)
            if pending is None or pending.future.done():
                raise ServiceError("host_reply_gone", "宿主请求已处理或不存在", 409)
            if pending.channel != envelope.channel:
                raise ServiceError("host_reply_mismatch", "宿主回复信道不匹配", 409)
            if pending.target != side:
                raise ServiceError("host_reply_side", "宿主回复方向不匹配", 409)
            pending.future.set_result(result)
        return ChannelResult(ok=True, data=None)

    async def events(
        self, identifier: str, side: HostSide, from_seq: int
    ) -> AsyncIterator[StreamFrame | None]:
        session = self.session(identifier)
        if from_seq < 0 or from_seq > len(session.frames[side]):
            raise ServiceError("host_cursor_invalid", "宿主事件游标无效", 416)
        cursor = from_seq
        while not session.closed:
            heartbeat = False
            async with session.condition:
                if cursor >= len(session.frames[side]):
                    try:
                        await asyncio.wait_for(session.condition.wait(), 15)
                    except TimeoutError:
                        heartbeat = True
                frames = session.frames[side][cursor:]
            if heartbeat:
                yield None
                continue
            for frame in frames:
                cursor = frame.seq + 1
                if side == "app" or (
                    isinstance(frame.data, dict) and frame.data.get("kind") == "call"
                ):
                    envelope = ProtocolEnvelope.model_validate(frame.data)
                    if envelope.id in session.delivered or (
                        envelope.kind == "call" and envelope.id not in session.pending
                    ):
                        envelope = ProtocolEnvelope(
                            kind="cancel", id=envelope.id, channel=envelope.channel, data=None
                        )
                        yield frame.model_copy(
                            update={
                                "data": envelope.model_dump(
                                    mode="json", exclude={"stream_id", "seq"}
                                )
                            }
                        )
                        continue
                    session.delivered.add(envelope.id)
                yield frame

    async def notify(self, identifier: str, envelope: ProtocolEnvelope) -> None:
        session = self.session(identifier)
        if envelope.kind != "notify" or envelope.channel not in {
            "host/state",
            "host/sidebar",
            "link/navigate",
        }:
            raise ServiceError("host_notification_invalid", "宿主通知信道无效", 422)
        if envelope.channel == "link/navigate":
            try:
                navigation = LinkNavigate.model_validate(envelope.data)
            except ValueError as error:
                raise ServiceError("link_invalid", "Link 通知 URL 无效", 422) from error
            if not session.connected:
                raise ServiceError("host_not_ready", "宿主 iframe 尚未就绪", 409)
            async with session.condition:
                if envelope.id in session.notifications:
                    raise ServiceError("link_duplicate", "Link 通知 ID 已使用", 409)
                if len(session.notifications) >= 512:
                    raise ServiceError("host_notification_limit", "宿主通知超过上限", 429)
                session.notifications.add(envelope.id)
            envelope = envelope.model_copy(
                update={
                    "data": TypeAdapter(JsonValue).validate_python(
                        navigation.model_dump(mode="json")
                    )
                }
            )
        await self.publish(session, "host", envelope)

    async def close(self, identifier: str) -> None:
        session = self._sessions.get(identifier)
        if session is None or session.closed:
            return
        async with session.condition:
            session.closed = True
            session.connected = False
            for pending in session.pending.values():
                if not pending.future.done():
                    pending.future.set_exception(
                        ServiceError("host_session_closed", "宿主控制会话已关闭", 410)
                    )
            session.condition.notify_all()

    async def shutdown(self) -> None:
        for identifier in list(self._sessions):
            await self.close(identifier)
        self._sessions.clear()
