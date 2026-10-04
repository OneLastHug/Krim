"""最小宿主门面；不提供 iframe、Skills 或完整上游兼容声明。"""

from .handle import AppHandle
from .remote import RemoteHostHandle

__all__ = ["AppHandle", "RemoteHostHandle"]
