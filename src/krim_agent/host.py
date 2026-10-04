"""Python Host 控制会话类型；消息复用内部 v2 信封。"""

from typing import Literal

from pydantic import Field

from .protocol.models import ProtocolEnvelope, StrictModel

HostSide = Literal["host", "app"]


class HostSessionView(StrictModel):
    id: str
    protocol_version: Literal[2] = 2
    connected: bool
    closed: bool
    expires_at: float


class HostCommand(StrictModel):
    envelope: ProtocolEnvelope
    timeout_seconds: float = Field(default=10, gt=0, le=300)


class HostUIState(StrictModel):
    connected: bool
    topic_id: str | None = None
    run_id: str | None = None
    iframe: bool
    locale: str


class HostThemeResult(StrictModel):
    theme: Literal["light", "dark", "system"]


class HostWidthResult(StrictModel):
    width: int = Field(ge=320, le=1600)


class HostSidebarResult(StrictModel):
    open: bool
