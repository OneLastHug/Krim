"""控制面到本机受限Broker的有界连接；不传宿主路径和登录凭据。"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import stat
import struct
from pathlib import Path
from typing import Any

from krim_agent.contracts import ServiceError

MAX_MESSAGE = 2 * 1024 * 1024


class BrokerClient:
    def __init__(self, path: Path, *, owner_uid: int = 0) -> None:
        self.path = path.absolute()
        self.owner_uid = owner_uid

    async def call(self, action: str, **arguments: Any) -> dict[str, Any]:
        try:
            if any(not stat.S_ISDIR(p.lstat().st_mode) for p in self.path.parents):
                raise ValueError("连接目录无效")
            info = self.path.lstat()
            if not stat.S_ISSOCK(info.st_mode) or info.st_uid != self.owner_uid:
                raise ValueError("连接所有者无效")
            if info.st_mode & 0o007 or (info.st_mode & 0o070 and info.st_gid != os.getegid()):
                raise ValueError("连接权限过宽或不属于当前服务组")
            payload = (
                json.dumps(
                    {"action": action, "arguments": arguments}, ensure_ascii=False, allow_nan=False
                ).encode()
                + b"\n"
            )
            if len(payload) > MAX_MESSAGE:
                raise ServiceError("invalid_input", 422)
            async with asyncio.timeout(20):
                reader, writer = await asyncio.open_unix_connection(
                    str(self.path), limit=MAX_MESSAGE + 1
                )
                try:
                    peer = writer.get_extra_info("socket")
                    if peer is None:
                        raise ValueError("缺少对端身份")
                    _, uid, _ = struct.unpack(
                        "3i",
                        peer.getsockopt(
                            socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")
                        ),
                    )
                    if uid != self.owner_uid:
                        raise ValueError("Broker进程身份无效")
                    writer.write(payload)
                    await writer.drain()
                    response = await reader.readline()
                    if not response.endswith(b"\n") or len(response) > MAX_MESSAGE:
                        raise ValueError("响应超限或未完成")
                    value = json.loads(response)
                    if not isinstance(value, dict) or set(value) not in (
                        {"result"},
                        {"error", "status"},
                    ):
                        raise ValueError("响应协议无效")
                    if "error" in value:
                        if type(value["error"]) is not str or type(value["status"]) is not int:
                            raise ValueError("错误协议无效")
                        raise ServiceError(value["error"], value["status"])
                    if not isinstance(value["result"], dict):
                        raise ValueError("结果协议无效")
                    return value["result"]
                finally:
                    writer.close()
                    await writer.wait_closed()
        except ServiceError:
            raise
        except (OSError, ValueError, TimeoutError, asyncio.IncompleteReadError):
            raise ServiceError("sandbox_backend_unavailable", 503) from None
