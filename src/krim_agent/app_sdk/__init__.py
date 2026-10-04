"""Python 宿主门面：进程内/同源 iframe、技能与上下文；不声明完整上游兼容。"""

from .handle import AppHandle
from .remote import RemoteHostHandle

__all__ = ["AppHandle", "RemoteHostHandle"]
