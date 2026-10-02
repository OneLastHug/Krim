"""Krim Agent 内部协议和进程内传输。"""

from .bridge import InProcessBridge
from .models import ChannelResult, ProtocolEnvelope, StreamFrame

__all__ = ["ChannelResult", "InProcessBridge", "ProtocolEnvelope", "StreamFrame"]
