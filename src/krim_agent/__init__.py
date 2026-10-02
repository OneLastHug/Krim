"""Krim Agent 的 Python 核心包。"""

__version__ = "0.1.0.dev0"

from .protocol.models import ChannelResult, ProtocolEnvelope, StreamFrame

__all__ = ["ChannelResult", "ProtocolEnvelope", "StreamFrame", "__version__"]
